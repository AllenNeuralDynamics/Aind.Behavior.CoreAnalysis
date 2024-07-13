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
    Optional,
    Union,
    overload,
)

import harp
import harp.reader
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
    DataStreamSource,
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
                    self._data = self._register_reader.read(
                        file=self._bin_file_inference_helper(path, self._register_reader, self.name),
                        keep_type=HarpDataStreamSourceBuilder._reader_default_params["keep_type"],  # internal
                        epoch=HarpDataStreamSourceBuilder._reader_default_params["epoch"],
                    )  # internal

            else:
                raise ValueError("reader method is not defined")
        return self._data

    @staticmethod
    def _bin_file_inference_helper(
        root_path: PathLike, registerReader: harp.reader.RegisterReader, name_hint: Optional[str] = None
    ) -> Path:

        root_path = Path(root_path)
        candidate_files = list(root_path.glob(f"*_{registerReader.register.address}.bin"))

        if name_hint is not None:  # If a name hint is provided, we can try to find it
            candidate_files += list(root_path.glob(f"*_{name_hint}.bin"))

        if len(candidate_files) == 1:
            return candidate_files[0]
        elif len(candidate_files) == 0:
            raise FileNotFoundError(
                "No binary file found for register while infering its location. Try passing the path explicitly"
            )
        else:
            raise ValueError(
                "Multiple binary files found for register while infering its location. Try passing the path explicitly"
            )


WhoAmI = NewType("WhoAmI", int)


class HarpDataStreamSourceBuilder(_DataStreamSourceBuilder):

    _reader_default_params = {
        "include_common_registers": True,
        "keep_type": True,
        "epoch": None,
    }  # Read-only

    _available_inference_modes = Literal["yml", "register_0"]  # Read-only

    def __init__(
        self,
        device_hint: Optional[DeviceReader | WhoAmI | PathLike] = None,
        default_inference_mode: _available_inference_modes = "yml",
    ) -> None:

        self.device_hint = device_hint
        self.default_inference_mode = default_inference_mode

    @overload
    def build(self, source: Optional[DataStreamSource] = None, /, **kwargs) -> StreamCollection: ...

    @overload
    def build(
        self, source: Optional[DataStreamSource] = None, /, path: Optional[PathLike] = None, **kwargs
    ) -> StreamCollection: ...

    def build(
        self, source: Optional[DataStreamSource] = None, /, path: Optional[PathLike] = None, **kwargs
    ) -> StreamCollection:

        # Leaving this undocumented here for now...
        device_hint = kwargs.get("device_hint", self.device_hint)
        default_inference_mode = kwargs.get("default_inference_mode", self.default_inference_mode)

        # User the explicit path if provided, otherwise default to the DataStreamSource object path
        if path is None:
            _source_path = source.path if source is not None else None
            if _source_path is not None:
                path = Path(_source_path)
            else:
                raise ValueError(
                    "Path is not defined. Define it in the DataStreamSource source object or pass it as an argument."
                )
        else:
            path = Path(path)

        if device_hint is None:
            match default_inference_mode:
                case "yml":
                    device_hint = harp.create_reader(device=path, **self._reader_default_params)
                case "register_0":
                    _reg_0_hint = list(path.glob("*_0.bin")) + list(path.glob("*whoami*.bin"))
                    if len(_reg_0_hint) == 0:
                        raise FileNotFoundError("register_0/whoami file not found")
                    else:
                        # Not sure why we would ever have more than one file, but defaulting to using the first
                        device_hint = int(harp.read(_reg_0_hint[0]).values[0][0])
                        new_builder = HarpDataStreamSourceBuilder(device_hint=device_hint)
                        return new_builder.build(
                            source, path=path
                        )  # Recursive call. Passing the source object for possible forward compatibility
                case _:
                    raise ValueError(
                        f"Invalid default_inference_mode. Must be one of \
                            {self._available_inference_modes}"
                    )

        elif isinstance(self.device_hint, DeviceReader):
            pass  # Trivially pass the device_reader object
        elif isinstance(self.device_hint, Path):
            self.device_hint = self._make_device_reader(path=path, file=self.device_hint)
        elif isinstance(self.device_hint, int):
            self.device_hint = self._get_reader_from_whoami(path=path, who_am_i=int(self.device_hint))
        else:
            raise ValueError("Invalid device reader input")

        if not isinstance(self.device_hint, DeviceReader):  # Guard-clause
            raise ValueError("Invalid device reader input")

        streams = StreamCollection()
        for name, reader in self.device_hint.registers.items():
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
                if "github.com" in url:
                    url = url.replace("github.com", "raw.githubusercontent.com")
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
