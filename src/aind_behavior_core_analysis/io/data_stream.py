from __future__ import annotations

import csv
import io
import json
from functools import cache
from os import PathLike
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Literal,
    NewType,
    NotRequired,
    Optional,
    TypedDict,
    Union,
    Unpack,
)

import harp
import pandas as pd
import requests
import yaml
from aind_behavior_services.data_types import SoftwareEvent
from harp.reader import (
    DeviceReader,
    RegisterReader,
    _create_register_parser,
    _ReaderParams,
)
from pydantic import BaseModel
from typing_extensions import override

from aind_behavior_core_analysis.io._core import (
    DataStream,
    StreamCollection,
    _DataStreamSourceBuilder,
)

DataFrameOrSeries = Union[pd.DataFrame, pd.Series]


class SoftwareEventStream(DataStream[DataFrameOrSeries]):
    """Represents a generic Software event."""

    def __init__(
        self,
        path: Optional[PathLike],
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        inner_parser: Optional[BaseModel] = None,
        **kwargs,
    ) -> None:
        super().__init__(path, name=name, auto_load=False, **kwargs)
        self._inner_parser = inner_parser
        self._run_auto_load(auto_load)

    def load(self, path: Optional[PathLike] = None, *, force_reload: bool = False, **kwargs) -> DataFrameOrSeries:
        super().load(path, force_reload=force_reload, **kwargs)
        self._data = self._apply_inner_parser(self._data)
        return self._data

    @classmethod
    def _file_reader(cls, path, *args, **kwargs) -> List[str]:
        with open(path, "r+", encoding="utf-8") as f:
            return f.readlines()

    @classmethod
    def _reader(
        cls,
        value: List[str],
        *args,
        validate: bool = True,
        pydantic_validate_kwargs: Optional[dict] = None,
        pydantic_model_dump_kwargs: Optional[dict] = None,
        **kwargs,
    ) -> DataFrameOrSeries:
        if not isinstance(value, list):
            value = [value]
        if validate:
            _entries = [
                SoftwareEvent.model_validate_json(
                    line, **(pydantic_validate_kwargs if pydantic_validate_kwargs else {})
                ).model_dump(**(pydantic_model_dump_kwargs if pydantic_model_dump_kwargs else {}))
                for line in value
            ]
        else:
            _entries = [json.loads(line) for line in value]

        df = pd.DataFrame(_entries)
        df.set_index("timestamp", inplace=True)

        return df

    def _apply_inner_parser(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._inner_parser is None:
            pass
        else:
            if df is None:
                raise ValueError("Data can not be None")
            if "data" not in df.columns:
                raise ValueError("data column not found")
            df["data"] = df.apply(lambda x: self._inner_parser.model_validate(x["data"]), axis=1)
        return df


class CsvStream(DataStream[DataFrameOrSeries]):
    """Represents a generic Software event."""

    def __init__(
        self, path: Optional[PathLike], *, name: Optional[str] = None, auto_load: bool = False, **kwargs
    ) -> None:
        super().__init__(path, name=name, auto_load=False, **kwargs)
        self._run_auto_load(auto_load)

    @classmethod
    def _file_reader(cls, path, *args, **kwargs) -> str:
        with open(path, "r+", encoding="utf-8") as f:
            return f.read()

    @classmethod
    def _reader(
        cls,
        value: str,
        *args,
        infer_index_col: Optional[str | int] = 0,
        col_names: Optional[List[str]] = None,
        **kwargs,
    ) -> DataFrameOrSeries:

        has_header = csv.Sniffer().has_header(value)
        _header = 0 if has_header is True else None
        df = pd.read_csv(io.StringIO(value), header=_header, index_col=infer_index_col, names=col_names)
        return df


class SingletonStream(DataStream[str | BaseModel]):
    """Represents a generic Software event."""

    def __init__(
        self,
        path: Optional[PathLike],
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        inner_parser: Optional[BaseModel] = None,
        **kwargs,
    ) -> None:
        super().__init__(path, name=name, auto_load=False, **kwargs)
        self._inner_parser = inner_parser
        self._run_auto_load(auto_load)

    @classmethod
    def _file_reader(cls, path, *args, **kwargs) -> str:
        with open(path, "r+", encoding="utf-8") as f:
            return f.read()

    @classmethod
    def _reader(cls, value, *args, **kwargs) -> str:
        return value

    def load(self, path: Optional[PathLike] = None, *, force_reload: bool = False, **kwargs) -> str | BaseModel:
        super().load(path, force_reload=force_reload, **kwargs)
        self._data = self._apply_inner_parser(self._data)
        return self._data

    def _apply_inner_parser(self, value: Optional[str | BaseModel]) -> str | BaseModel:
        if value is None:
            raise ValueError("Data can not be None")
        if self._inner_parser is None:
            return value
        else:
            if isinstance(value, str):
                return self._inner_parser.model_validate_json(value)
            elif isinstance(value, BaseModel):
                return self._inner_parser.model_validate(value.model_dump())
            else:
                raise TypeError("Invalid type")


class HarpDataStream(DataStream[DataFrameOrSeries]):

    def __init__(
        self,
        path: Optional[PathLike],
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        register_reader: Optional[RegisterReader] = None,
        **kwargs,
    ) -> None:
        super().__init__(path, name=name, auto_load=False, **kwargs)
        self._register_reader = register_reader
        self._run_auto_load(auto_load)

    @classmethod
    def _file_reader(cls, path, *args, **kwargs) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    @classmethod
    def _reader(cls, *args, **kwargs) -> DataFrameOrSeries:
        return harp.read(*args, **kwargs)

    @override
    def load(self, path: Optional[PathLike] = None, *, force_reload: bool = False, **kwargs) -> DataFrameOrSeries:
        if force_reload is False and self._data:
            pass
        else:
            path = Path(path) if path is not None else self.path
            if path:
                self.path = path
                if self._register_reader is None:
                    self._data = self._reader(path)
                else:
                    self._data = self._register_reader.read(keep_type=True)

            else:
                raise ValueError("reader method is not defined")
        return self._data


WhoAmI = NewType("WhoAmI", int)


class HarpDataStreamSourceBuilder(_DataStreamSourceBuilder):

    _reader_default_params = {
        "include_common_registers": True,
        "keep_type": True,
        "epoch": None,
    }  # Read-only

    class _BuilderInputSignature(TypedDict, total=False):
        path: PathLike
        device_hint: NotRequired[DeviceReader | WhoAmI | PathLike]
        default_inference_mode: NotRequired[Literal["yml", "register0"]]

    @override
    @classmethod
    def build(cls, **build_kwargs: Unpack[_BuilderInputSignature]) -> StreamCollection:

        path = build_kwargs.get("path", None)
        if path is None:
            raise ValueError("path is required")
        path = Path(path)

        device_reader = build_kwargs.get("device_hint", None)

        if device_reader is None:
            default_inference_mode = build_kwargs.get("default_inference_mode", "yml")
            match default_inference_mode:
                case "yml":
                    device_reader = harp.create_reader(device=path, **cls._reader_default_params)
                case "register0":
                    raise NotImplementedError("register0 inference mode not implemented yet")
                case _:
                    raise ValueError(
                        f"Invalid default_inference_mode. Must be one of \
                            {cls._BuilderInputSignature['default_inference_mode']}"
                    )

        elif isinstance(device_reader, DeviceReader):
            pass  # Trivially pass the device_reader object
        elif isinstance(device_reader, Path):
            device_reader = cls._make_device_reader(path=path, file=device_reader)
        elif isinstance(device_reader, int):
            device_reader = cls._get_reader_from_whoami(path=path, who_am_i=int(device_reader))
        else:
            raise ValueError("Invalid device reader input")

        if not isinstance(device_reader, DeviceReader):  # Guard clause just in case. Fail early.
            raise TypeError(f"Invalid device_reader type. Expected {type(DeviceReader)} but got {type(device_reader)}")

        streams = StreamCollection()
        for name, reader in device_reader.registers.items():
            streams.try_append(name, HarpDataStream(path, name=name, register_reader=reader, auto_load=False))
        return streams

    @classmethod
    def _make_device_reader(cls, path: PathLike, file: str | PathLike | io.TextIO) -> DeviceReader:
        device = harp.read_schema(file, include_common_registers=cls._reader_default_params["include_common_registers"])
        reg_readers = {
            name: _create_register_parser(
                device,
                name,
                _ReaderParams(path, cls._reader_default_params["epoch"], cls._reader_default_params["keep_type"]),
            )
            for name in device.registers.keys()
        }
        return DeviceReader(device, reg_readers)

    @classmethod
    def _get_reader_from_whoami(cls, path: PathLike, who_am_i: int, release: str = "main") -> DeviceReader:
        yml_stream = io.TextIOWrapper(cls._get_yml_from_who_am_i(who_am_i, release=release))
        return cls._make_device_reader(path, yml_stream)

    @classmethod
    def _get_yml_from_who_am_i(cls, who_am_i: int, release: str = "main") -> io.BytesIO:
        try:
            device = cls._get_who_am_i_list()[who_am_i]
        except KeyError as e:
            raise KeyError(f"WhoAmI {who_am_i} not found in whoami.yml") from e

        repository_url = device.get("repositoryUrl", None)

        if repository_url is None:
            raise ValueError("Device's repositoryUrl not found in whoami.yml")
        else:  # attempt to get the device.yml from the repository
            _repo_hint_paths = [
                "{repository_url}/{release}/device.yml",
                "{repository_url}/{release}/software/bonsai/device.yml",
            ]

            yml = None
            for hint in _repo_hint_paths:
                url = hint.format(repository_url=repository_url, release=release)
                response = requests.get(url, allow_redirects=True, timeout=5)
                if response.status_code == 200:
                    yml = io.BytesIO(response.content)
                    break
            if yml is None:
                raise FileNotFoundError("device.yml not found in any repository")
            else:
                return yml

    @cache
    @staticmethod
    def _get_who_am_i_list(
        url: str = "https://raw.githubusercontent.com/harp-tech/protocol/main/whoami.yml",
    ) -> Dict[int, Any]:
        response = requests.get(url, allow_redirects=True, timeout=5)
        content = response.content.decode("utf-8")
        content = yaml.safe_load(content)
        devices = content["devices"]
        return devices

    @classmethod
    def parse_kwargs(cls, kwargs: dict[str, Any]) -> _BuilderInputSignature:
        return cls._BuilderInputSignature(**kwargs)  # type: ignore
