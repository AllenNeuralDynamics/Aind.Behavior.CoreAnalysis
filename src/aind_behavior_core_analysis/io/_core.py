from __future__ import annotations

import abc
from collections import UserDict
from os import PathLike
from pathlib import Path
from typing import (
    Any,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    Unpack,
    overload,
)

from aind_behavior_core_analysis.io._utils import StrPattern, validate_str_pattern

TData = TypeVar("TData", bound=Any)


class DataStream(abc.ABC, Generic[TData]):

    def __init__(
        self,
        path: Optional[PathLike],
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        _data: Optional[TData] = None,
        **kwargs,
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

        self._run_auto_load(auto_load)

    def _run_auto_load(self, override_to: bool) -> None:
        if override_to is True:
            self._auto_load = True
            self.load()
        else:
            pass  # Defer decision to the child class

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
    def _file_reader(cls, path: PathLike, *args, **kwargs) -> Any:
        pass

    @classmethod
    @abc.abstractmethod
    def _reader(cls, value, *args, **kwargs) -> TData:
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

    def load(self, path: Optional[PathLike] = None, *, force_reload: bool = False, **kwargs) -> TData:

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

    def __str__(self) -> str:
        return f"{self.__class__.__name__} stream with data{'' if self._data is not None else 'not'} loaded."


class _DataStreamSourceBuilder(abc.ABC):

    class _BuilderInputSignature(TypedDict, total=False):
        pass

    @abc.abstractmethod
    def build(self, **build_kwargs: Unpack[_BuilderInputSignature]) -> StreamCollection: ...

    @classmethod
    def parse_kwargs(cls, kwargs: dict[str, Any]) -> _BuilderInputSignature:
        return cls._BuilderInputSignature(**kwargs)  # type: ignore


_SequenceDataStreamBuilderPattern = Sequence[Tuple[Type[DataStream], StrPattern]]


class DataStreamSource:
    """Represents a data stream source, usually comprised of various files from a single folder.
    These folders usually result from a single data acquisition logger"""

    @overload
    def __init__(
        self,
        path: PathLike,
        builder: Type[DataStream],
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        **kwargs,
    ) -> None: ...

    @overload
    def __init__(
        self,
        path: PathLike,
        builder: _SequenceDataStreamBuilderPattern,
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        **kwargs,
    ) -> None: ...

    @overload
    def __init__(
        self,
        path: PathLike,
        *,
        builder: _DataStreamSourceBuilder,
        name: Optional[str] = None,
        auto_load: bool = False,
        **kwargs,
    ) -> None: ...

    @overload
    def __init__(
        self,
        path: PathLike,
        builder: None,
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        **kwargs,
    ) -> None: ...

    def __init__(
        self,
        path: PathLike,
        builder: None | Type[DataStream] | _SequenceDataStreamBuilderPattern | _DataStreamSourceBuilder = None,
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        **kwargs,
    ) -> None:

        self._streams: StreamCollection
        self._path = Path(path)

        if not self._path.is_dir():
            raise FileExistsError(f"Path {self._path} is not a directory")
        self._name = name if name else self._path.name

        #  Build the StreamCollection object
        self._builder = builder

        if self._builder is None:
            raise NotImplementedError(
                "builder must not be provided. Support for automatic inference is not yet implemented."
            )
        if isinstance(self._builder, _DataStreamSourceBuilder):
            self._streams = self._builder.build(**self._builder.parse_kwargs(kwargs))

        elif isinstance(self._builder, (type(DataStream), Sequence)):
            self._builder = self._normalize_builder_from_data_stream(self._builder)
            self._streams = self._build_from_data_stream(self.path, self._builder)

        else:
            raise TypeError("Builder type is not supported.")

        if auto_load is True:
            self.reload_streams()

    @staticmethod
    def _normalize_builder_from_data_stream(builder: Type[DataStream] | Sequence) -> _SequenceDataStreamBuilderPattern:
        _builder: Sequence
        if isinstance(builder, type(DataStream)):  # If only a single data stream class is provided
            _builder = ((builder, "*"),)

        for _tuple in _builder:
            if not isinstance(_tuple[0], type(DataStream)):
                raise ValueError("builder must be a DataStream type")
            validate_str_pattern(_tuple[1])
        return _builder

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

    @staticmethod
    def _get_data_streams_helper(
        path: PathLike, stream_type: Type[DataStream], pattern: StrPattern
    ) -> List[DataStream]:
        _path = Path(path)
        if isinstance(pattern, str):
            pattern = [pattern]
        files: List[Path] = []
        for pat in pattern:
            files.extend(_path.glob(pat))
        files = list(set(files))
        streams: List[DataStream] = [stream_type(file) for file in files]
        return streams

    @classmethod
    def _build_from_data_stream(cls, path: PathLike, builder: _SequenceDataStreamBuilderPattern) -> StreamCollection:
        streams = StreamCollection()
        for stream_type, pattern in builder:
            _this_type_stream = cls._get_data_streams_helper(path, stream_type, pattern)
            for stream in _this_type_stream:
                if stream.name is None:
                    raise ValueError(f"Stream {stream} does not have a name")
                else:
                    streams.try_append(stream.name, stream)
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

    def try_append(self, key: str, value: DataStream) -> None:
        """
        Tries to append a key-value pair to the dictionary.

        Args:
            key (str): The key to be appended.
            value (DataStream): The value to be appended.

        Raises:
            KeyError: If the key already exists in the dictionary.
        """
        if key in self:
            raise KeyError(f"Key {key} already exists in dictionary")
        else:
            self[key] = value
