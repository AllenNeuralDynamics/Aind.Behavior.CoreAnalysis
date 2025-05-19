import datetime
import io
import os
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Dict, List, Literal, Optional, Self, TextIO, Tuple, TypeAlias, Union

import harp
import harp.reader
import pandas as pd
import requests
import yaml
from pydantic import AnyHttpUrl, BaseModel, Field, dataclasses
from typing_extensions import TypeAliasType, override

from .. import _typing
from ._core import DataStream, DataStreamCollectionBase, FilePathBaseParam

HarpRegisterParams: TypeAlias = harp.reader._ReaderParams

_DEFAULT_HARP_READER_PARAMS = HarpRegisterParams(base_path=None, epoch=None, keep_type=True)


class HarpRegister(DataStream[pd.DataFrame, HarpRegisterParams]):
    make_params = HarpRegisterParams

    @override
    def read(self, reader_params: Optional[HarpRegisterParams] = None) -> pd.DataFrame:
        reader_params = reader_params if reader_params is not None else self._reader_params
        if _typing.is_unset(reader_params):
            raise ValueError("Reader parameters are not set. Cannot read data.")
        return self._reader(reader_params.base_path, epoch=reader_params.epoch, keep_type=reader_params.keep_type)

    @classmethod
    def from_register_reader(
        cls,
        name: str,
        reg_reader: harp.reader.RegisterReader,
        params: HarpRegisterParams = _DEFAULT_HARP_READER_PARAMS,
    ) -> Self:
        c = cls(
            name=name,
            description=reg_reader.register.description,
        )
        c.bind_reader_params(params)
        c._reader = reg_reader.read
        c.make_params = cls.make_params
        return c


class _DeviceYmlSource(BaseModel):
    method: str


class DeviceYmlByWhoAmI(_DeviceYmlSource):
    method: Literal["whoami"] = "whoami"
    who_am_i: Annotated[int, Field(ge=0, le=9999, description="WhoAmI value")]


class DeviceYmlByFile(_DeviceYmlSource):
    method: Literal["file"] = "file"
    path: Optional[os.PathLike | str] = Field(default=None, description="Path to the device yml file")


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
class HarpDeviceParams(FilePathBaseParam):
    device_yml_hint: DeviceYmlSource = Field(
        default=DeviceYmlByFile(), description="Device yml hint", validate_default=True
    )
    include_common_registers: bool = Field(default=True, description="Include common registers")
    keep_type: bool = Field(default=True, description="Keep message type information")
    epoch: Optional[datetime.datetime] = Field(
        default=None,
        description="Reference datetime at which time zero begins. If specified, the result data frame will have a datetime index.",
    )


def _harp_device_reader(
    params: HarpDeviceParams,
) -> Tuple[List[HarpRegister], harp.reader.DeviceReader]:
    _yml_stream: str | os.PathLike | TextIO
    match params.device_yml_hint:
        case DeviceYmlByWhoAmI(who_am_i=who_am_i):
            # If WhoAmI is provided we xref it to the device list to find the correct device.yml
            _yml_stream = io.TextIOWrapper(fetch_yml_from_who_am_i(who_am_i))

        case DeviceYmlByRegister0(register0_glob_pattern=glob_pattern):
            # If we are allowed to infer the WhoAmI, we try to find it
            _reg_0_hint: List[os.PathLike] = []
            for pattern in glob_pattern:
                _reg_0_hint.extend(Path(params.path).glob(pattern))
            if len(_reg_0_hint) == 0:
                raise FileNotFoundError(
                    "File corresponding to WhoAmI register not found given the provided glob patterns."
                )
            device_hint = int(
                harp.read(_reg_0_hint[0]).values[0][0]
            )  # We read the first line of the file to get the WhoAmI value
            _yml_stream = io.TextIOWrapper(fetch_yml_from_who_am_i(device_hint))

        case DeviceYmlByFile(path=path):
            # If a device.yml is provided we trivially pass it to the reader
            if path is None:
                path = Path(params.path) / "device.yml"
            else:
                path = Path(path)
            _yml_stream = io.TextIOWrapper(open(path, "rb"))

        case DeviceYmlByUrl(url=url):
            # If a device.yml URL is provided we fetch it and pass it to the reader
            response = requests.get(url, allow_redirects=True, timeout=5)
            response.raise_for_status()
            if response.status_code == 200:
                _yml_stream = io.TextIOWrapper(io.BytesIO(response.content))
            else:
                raise ValueError(f"Failed to fetch device yml from {url}")

        case _:
            raise ValueError("Invalid device yml hint")

    reader = _make_device_reader(_yml_stream, params)
    data_streams: List[HarpRegister] = []

    for name, reg_reader in reader.registers.items():
        # todo we can add custom file name interpolation here
        data_streams.append(HarpRegister.from_register_reader(name, reg_reader, _DEFAULT_HARP_READER_PARAMS))
    return (data_streams, reader)


def _make_device_reader(yml_stream: str | os.PathLike | TextIO, params: HarpDeviceParams) -> harp.reader.DeviceReader:
    device = harp.read_schema(yml_stream, include_common_registers=params.include_common_registers)
    path = Path(params.path)
    base_path = path / device.device if path.is_dir() else path.parent / device.device
    reg_readers = {
        name: harp.reader._create_register_handler(
            device,
            name,
            HarpRegisterParams(base_path=base_path, epoch=params.epoch, keep_type=params.keep_type),
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
    url: str = "https://raw.githubusercontent.com/harp-tech/whoami/main/whoami.yml",
) -> Dict[int, Any]:
    response = requests.get(url, allow_redirects=True, timeout=5)
    content = response.content.decode("utf-8")
    content = yaml.safe_load(content)
    devices = content["devices"]
    return devices


class HarpDevice(DataStreamCollectionBase[HarpRegister, HarpDeviceParams]):
    make_params = HarpDeviceParams
    _device_reader: Optional[harp.reader.DeviceReader]

    @property
    def device_reader(self) -> harp.reader.DeviceReader:
        if not hasattr(self, "_device_reader"):
            raise ValueError("Device reader is not set. Cannot read data.")
        if self._device_reader is None:
            raise ValueError("Device reader is not set. Cannot read data.")
        return self._device_reader

    def _reader(self, params: HarpDeviceParams) -> List[HarpRegister]:
        regs, reader = _harp_device_reader(params)
        self._device_reader = reader
        return regs
