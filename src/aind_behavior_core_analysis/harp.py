import datetime
import io
import os
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Dict, List, Literal, Optional, TextIO, Union
from aind_behavior_core_analysis import _typing

import harp
import harp.reader
import pandas as pd
import requests
import yaml
from pydantic import AnyHttpUrl, BaseModel, Field
from typing_extensions import TypeAliasType

from aind_behavior_core_analysis._core import DataStream
import dataclasses


class _DeviceYmlSource(BaseModel):
    method: str


class DeviceYmlByWhoAmI(_DeviceYmlSource):
    method: Literal["whoami"] = "whoami"
    who_am_i: Annotated[int, Field(ge=0, le=9999, description="WhoAmI value")]


class DeviceYmlByFile(_DeviceYmlSource):
    method: Literal["file"] = "file"
    path: os.PathLike | str = Field(default=".", description="Path to the device yml file")


class DeviceYmlByUrl(_DeviceYmlSource):
    method: Literal["http"] = "http"
    url: AnyHttpUrl = Field(description="URL to the device yml file")


class DeviceYmlByRegister0(_DeviceYmlSource):
    method: Literal["register0"] = "register0"
    register0_glob_pattern: List[str] = Field(
        default=["*_0.bin", "*whoami*.bin"],
        min_length=1,
        description="Glob pattern to match the WhoAmI (0) register file",
    )


if TYPE_CHECKING:
    DeviceYmlSource = Union[DeviceYmlByWhoAmI, DeviceYmlByFile, DeviceYmlByUrl, DeviceYmlByRegister0]
else:
    DeviceYmlSource: TypeAliasType = Annotated[
        Union[DeviceYmlByWhoAmI, DeviceYmlByFile, DeviceYmlByUrl, DeviceYmlByRegister0], Field(discriminator="method")
    ]


@dataclasses.dataclass
class HarpDeviceReaderParams:
    path: os.PathLike
    device_yml_hint: DeviceYmlSource = Field(
        default=DeviceYmlByFile(), description="Device yml hint", validate_default=True
    )
    include_common_registers: bool = Field(default=True, description="Include common registers")
    keep_type: bool = Field(default=True, description="Keep message type information")
    epoch: Optional[datetime.datetime] = Field(
        default=None,
        description="Reference datetime at which time zero begins. If specified, the result data frame will have a datetime index.",
    )


def harp_device_reader(
    params: HarpDeviceReaderParams,
) -> Dict[str, DataStream[pd.DataFrame, harp.reader._ReaderParams, _typing.NullParams]]:
    _yml_stream: str | os.PathLike | TextIO

    # If WhoAmI is provided we xref it to the device list to find the correct device.yml
    if isinstance(params.device_yml_hint, DeviceYmlByWhoAmI):
        _yml_stream = io.TextIOWrapper(fetch_yml_from_who_am_i(params.device_yml_hint.who_am_i))

    # If we are allowed to infer the WhoAmI, we try to find it, and
    # we it is simply a subset of the previous case
    elif isinstance(params.device_yml_hint, DeviceYmlByRegister0):
        _reg_0_hint: List[os.PathLike] = []
        for pattern in params.device_yml_hint.register0_glob_pattern:
            _reg_0_hint.extend(Path(params.path).glob(pattern))
        if len(_reg_0_hint) == 0:
            raise FileNotFoundError("File corresponding to WhoAmI register not found given the provided glob patterns.")
        device_hint = int(
            harp.read(_reg_0_hint[0]).values[0][0]
        )  # We read the first line of the file to get the WhoAmI value
        _yml_stream = io.TextIOWrapper(fetch_yml_from_who_am_i(device_hint))

    # If a device.yml is provided we trivially pass it to the reader
    elif isinstance(params.device_yml_hint, DeviceYmlByFile):
        with open(params.device_yml_hint.path, "r", encoding="utf-8") as file:
            _yml_stream = file.read()

    # If a device.yml URL is provided we fetch it and pass it to the reader
    elif isinstance(params.device_yml_hint, DeviceYmlByUrl):
        response = requests.get(params.device_yml_hint.url, allow_redirects=True, timeout=5)
        response.raise_for_status()
        if response.status_code == 200:
            _yml_stream = io.TextIOWrapper(io.BytesIO(response.content))
        else:
            raise ValueError(f"Failed to fetch device yml from {params.device_yml_hint.url}")

    else:
        raise ValueError("Invalid device yml hint")

    reader = _make_device_reader(_yml_stream, params)
    data_streams: Dict[str, DataStream[pd.DataFrame, harp.reader._ReaderParams, _typing.NullParams]] = {}

    for name, reader in reader.registers.items():
        # todo we can add custom file name interpolation here
        def _reader(params: harp.reader._ReaderParams):
            return reader.read(file_or_buf=params.base_path, epoch=params.epoch, keep_type=params.keep_type)

        data_streams[name] = DataStream(
            reader=_reader,
            reader_params=harp.reader._ReaderParams(base_path=None, epoch=params.epoch, keep_type=params.keep_type),
        )
    return data_streams


def _make_device_reader(
    yml_stream: str | os.PathLike | TextIO, params: HarpDeviceReaderParams
) -> harp.reader.DeviceReader:
    device = harp.read_schema(yml_stream, include_common_registers=params.include_common_registers)
    path = Path(params.path)
    base_path = path / device.device if path.is_dir() else path.parent / device.device
    reg_readers = {
        name: harp.reader._create_register_handler(
            device,
            name,
            harp.reader._ReaderParams(base_path, params.epoch, params.keep_type),
        )
        for name in device.registers.keys()
    }
    return harp.reader.DeviceReader(device, reg_readers)


def fetch_yml_from_who_am_i(who_am_i: int, release: str = "main") -> io.BytesIO:
    try:
        device = fetch_who_am_i_list()[who_am_i]
    except KeyError as e:
        raise KeyError(f"WhoAmI {who_am_i} not found in whoami.yml") from e

    repository_url = device.get("repositoryUrl", None)

    if repository_url is None:
        raise ValueError("Device's repositoryUrl not found in whoami.yml")

    _repo_hint_paths = [
        "{repository_url}/{release}/device.yml",
        "{repository_url}/{release}/software/bonsai/device.yml",
    ]

    yml = None
    for hint in _repo_hint_paths:
        url = hint.format(repository_url=repository_url, release=release)
        if "github.com" in url:
            url = url.replace("github.com", "raw.githubusercontent.com")
        response = requests.get(url, allow_redirects=True, timeout=5)
        if response.status_code == 200:
            yml = io.BytesIO(response.content)
            return yml

    raise ValueError("device.yml not found in any repository")


@cache
def fetch_who_am_i_list(
    url: str = "https://raw.githubusercontent.com/harp-tech/protocol/main/whoami.yml",
) -> Dict[int, Any]:
    response = requests.get(url, allow_redirects=True, timeout=5)
    content = response.content.decode("utf-8")
    content = yaml.safe_load(content)
    devices = content["devices"]
    return devices
