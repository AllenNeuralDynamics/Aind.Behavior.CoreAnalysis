from __future__ import annotations

import abc
import re
from collections import UserDict
from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path
from typing import (
    Any,
    Generic,
    List,
    Mapping,
    NewType,
    Optional,
    Self,
    Type,
    TypeVar,
    Union,
)

TData = TypeVar("TData", bound=Any)


class DataStream(abc.ABC, Generic[TData]):

    def __init__(
        self,
        path: Optional[PathLike],
        name: Optional[str] = None,
        auto_load: bool = False,
        _data: Optional[TData] = None,
    ) -> None:

        self._auto_load = auto_load
        self._data = _data

        if path is not None:
            path = Path(path)
            self._path = path
            if not self._path.is_file():
                raise FileExistsError(f"Path {self._path} is not a file")
            self._name = name if name is not None else path.stem
        else:
            if name is None:
                raise ValueError("Either path or name must be provided")

        if self._auto_load is True:
            self.load()

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> Optional[Path]:
        return self._path

    @path.setter
    def path(self, value: PathLike) -> None:
        _path = Path(value)
        if not _path.is_file():
            raise FileExistsError(f"Path {self._path} is not a file")
        self._path = Path(value)

    @classmethod
    @abc.abstractmethod
    def _file_reader(cls, path: PathLike):
        pass

    @classmethod
    @abc.abstractmethod
    def _reader(cls, value) -> TData:
        pass

    @classmethod
    @abc.abstractmethod
    def _parser(cls, value: Any) -> TData:
        pass

    @property
    def data(self) -> TData:
        """Returns the data"""
        if self._data is None:
            raise ValueError(
                "Data is not loaded. \
                             Try self.load() to attempt\
                             to automatically load it"
            )
        return self._data

    def load(self, path: Optional[PathLike] = None, force_reload: bool = False) -> TData:

        if force_reload is False and self._data:
            pass
        else:
            path = Path(path) if path is not None else self.path
            if path:
                self.path = path
                self._data = self._reader(self._file_reader(path))

            else:
                raise ValueError("reader method is not defined")
        return self._data

    @classmethod
    def parse(cls, value: Any, **kwargs) -> Self:
        """Loads the data stream from a value"""
        ds = cls(**kwargs)
        if ds._parser is not None:
            ds._data = ds._parser(value)
            return ds
        else:
            raise NotImplementedError("A valid ._parse method must be implemented")

    def __str__(self) -> str:
        return f"{self.__class__.__name__} stream with data{'' if self._data is not None else 'not'} loaded."


StrPattern = NewType("StrPattern", str)


def validate_str_pattern(pattern: Union[StrPattern, List[StrPattern]]) -> None:
    if isinstance(pattern, list):
        for pat in pattern:
            validate_str_pattern(pat)
    else:
        try:
            re.compile(pattern)
        except re.error as err:
            raise re.error(f"Pattern {pattern} is not a valid regex pattern") from err


class DataStreamSource:
    """Represents a data stream source, usually comprised of various files from a single folder.
    These folders usually result from a single data acquisition logger"""

    def __init__(
        self,
        path: PathLike,
        name: Optional[str] = None,
        data_stream_map: Union[Type[DataStream], Mapping[Type[DataStream], StrPattern]] = DataStream,
        auto_load: bool = False,
    ) -> None:

        self._path = Path(path)

        if not self._path.is_dir():
            raise FileExistsError(f"Path {self._path} is not a directory")
        self._name = name if name else self._path.name

        # TODO Support automatic inference in the future
        self._data_stream_map: Mapping[Type[DataStream], StrPattern]

        if not data_stream_map:
            raise NotImplementedError(
                "data_stream_map must be provided. Support for automatic inference is not yet implemented"
            )

        if isinstance(data_stream_map, DataStream):  # If only a single data stream class is provided
            data_stream_map = {data_stream_map: "*"}

        if not isinstance(data_stream_map, Mapping):
            raise ValueError("data_stream_map must be a Mapping or a DataStream class")

        validate_str_pattern(list(data_stream_map.values()))
        self._data_stream_map = data_stream_map
        self._streams = self._get_streams()

        if auto_load is True:
            self.reload_streams()

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> Path:
        if self._path is None:
            raise ValueError("Path is not defined")
        return Path(self._path)

    @property
    def streams(self) -> StreamCollection:
        return self._streams

    def _get_stream_file_paths(self, pattern: str) -> List[Path]:
        _path = Path(self.path)
        return list(_path.glob(pattern))

    def _get_streams_from_type(self, stream_type: Type[DataStream], pattern: str) -> List[DataStream]:
        files = self._get_stream_file_paths(pattern)
        streams: List[DataStream] = [stream_type(file) for file in files]
        return streams

    def _get_streams(self) -> StreamCollection:
        streams = StreamCollection()
        for stream_type, pattern in self._data_stream_map.items():
            _this_type_stream = self._get_streams_from_type(stream_type, pattern)
            for stream in _this_type_stream:
                if stream.name is None:
                    raise ValueError(f"Stream {stream} does not have a name")
                else:
                    streams.validate_and_append(stream.name, stream)
        return streams

    def reload_streams(self, force_reload: bool = False) -> None:
        for stream in self.streams.values():
            stream.load(force_reload=force_reload)

    def __str__(self) -> str:
        return f"DataStreamSource from {self._path}"

    def __repr__(self) -> str:
        return f"DataStreamSource from {self._path}"


class StreamCollection(UserDict[str, DataStream]):
    def __str__(self):
        single_streams = [f"{key}: {value}" for key, value in self.items()]
        return f"Streams with {len(self)} streams: \n" + "\n".join(single_streams)

    def validate_and_append(self, key: str, value: DataStream) -> None:
        if key in self:
            raise KeyError(f"Key {key} already exists in dictionary")
        else:
            self[key] = value
