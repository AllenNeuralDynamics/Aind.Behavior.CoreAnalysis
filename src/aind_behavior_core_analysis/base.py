import abc
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, Generic, Optional, Protocol, Self, TypeAlias, TypeVar, Union, final

import pydantic
from pydantic import BaseModel, Field, computed_field

# solve paths posthoc by iterating down the tree
_TData = TypeVar("_TData", bound=Any)

_ReaderParams = TypeVar("_ReaderParams", bound=BaseModel, contravariant=True)
_WriterParams = TypeVar("_WriterParams", bound=BaseModel, contravariant=True)
_co_TData = TypeVar("_co_TData", covariant=True, bound=Any)
_contra_TData = TypeVar("_contra_TData", contravariant=True, bound=Any)


@pydantic.dataclasses.dataclass
class DataStreamParams(abc.ABC, Generic[_ReaderParams, _WriterParams]):
    """Serializable model that represents a data stream in a dataset."""

    name: str = Field(description="Name of the data stream")
    path: os.PathLike = Field(description="Absolute or relative path to the data stream")
    reader: _ReaderParams = Field(description="Reader parameters")
    writer: _WriterParams = Field(description="Writer parameters")

    @computed_field(description="Type of the data stream")
    def data_stream_type(self) -> str:
        return self.__class__.__name__


class _Reader(Protocol, Generic[_co_TData, _ReaderParams]):
    def __call__(self, params: _ReaderParams) -> _co_TData: ...


class _Writer(Protocol, Generic[_contra_TData, _WriterParams]):
    def __call__(self, data: _contra_TData, params: _WriterParams) -> Any: ...


class DataStreamBuilder(abc.ABC, Generic[_TData, _ReaderParams, _WriterParams]):
    _reader: Optional[_Reader[_TData, _ReaderParams]]
    _writer: Optional[_Writer[_TData, _WriterParams]]

    def __init__(
        self,
        reader: Optional[_Reader[_TData, _ReaderParams]] = None,
        writer: Optional[_Writer[_TData, _WriterParams]] = None,
    ) -> None:
        self._reader = reader
        self._writer = writer

    def read(self, params: _ReaderParams) -> _TData:
        """Read data from the data stream."""
        if self._reader is None:
            raise ValueError("_reader callable is not set.")
        return self._reader(params)

    def write(self, data: _TData, params: _WriterParams) -> None:
        """Write data to the data stream."""
        if self._writer is None:
            raise ValueError("_writer callable is not set.")
        return self._writer(data, params)

    def build(self: Self, reader_params: _ReaderParams, writer_params: _WriterParams):
        """Build a data stream from the given parameters."""
        return DataStream[_TData, _ReaderParams, _WriterParams](
            io=self,
            reader_params=reader_params,
            writer_params=writer_params,
            read_on_init=False,
        )


@dataclass
class DataStream(abc.ABC, Generic[_TData, _ReaderParams, _WriterParams]):
    io: DataStreamBuilder[_TData, _ReaderParams, _WriterParams]
    reader_params: _ReaderParams
    writer_params: _WriterParams
    read_on_init: bool = field(default=False, init=True, repr=False, kw_only=True)
    _data: Optional[_TData] = field(default=None, init=True, repr=False, kw_only=True)

    def __post_init__(self) -> None:
        if self.read_on_init:
            self.load()

    @property
    def data(self) -> _TData:
        if self._data is None:
            raise ValueError("Data has not been read yet.")
        return self._data

    def load(self) -> None:
        """Load data into the data stream."""
        self._data = self.read()

    def read(self) -> _TData:
        """Read data from the data stream."""
        return self.io.read(self.reader_params)

    def write(self, data: Optional[_TData] = None) -> None:
        if data is None:
            self.io.write(self.data, self.writer_params)


_GroupType: TypeAlias = Dict[str, Union[DataStream, "DataStreamGroup"]]


class DataStreamGroupBuilder(
    DataStreamBuilder[_GroupType, _ReaderParams, _WriterParams], Generic[_ReaderParams, _WriterParams]
):
    def build(self: Self, reader_params: _ReaderParams, writer_params: _WriterParams):
        """Build a data stream from the given parameters."""
        return DataStreamGroup[_ReaderParams, _WriterParams](
            io=self,
            reader_params=reader_params,
            writer_params=writer_params,
            read_on_init=False,
        )


@final
class _UndefinedParams(BaseModel):
    pass


@dataclass
class DataStreamGroup(Generic[_ReaderParams, _WriterParams]):
    io: DataStreamGroupBuilder[_ReaderParams, _WriterParams]
    reader_params: _ReaderParams
    writer_params: _WriterParams
    read_on_init: bool = field(default=False, init=True, repr=False, kw_only=True)
    _data_streams: Optional[_GroupType] = field(default=None, init=True, repr=False, kw_only=True)

    def __post_init__(self) -> None:
        if self.read_on_init:
            self.load()

    @property
    def data_streams(self) -> _GroupType:
        if self._data_streams is None:
            raise ValueError("Data has not been read yet.")
        return self._data_streams

    def load(self) -> None:
        """Load data into the data stream."""
        self._data_streams = self.read()

    def read(self) -> _GroupType:
        """Read data from the data stream."""
        return self.io.read(self.reader_params)

    def __getitem__(self, key: str) -> Any:
        if self._data_streams is None:
            raise ValueError("Data streams have not been loaded yet.")
        if key in self._data_streams:
            return self._data_streams[key]
        else:
            raise KeyError(f"Key '{key}' not found in data streams.")

    def __str__(self):
        table = []
        table.append(["Stream Name", "Stream Type", "Is Loaded"])
        table.append(["-" * 20, "-" * 20, "-" * 20])
        for key, value in self.data_streams.items():
            table.append([key, value.__class__.__name__, "Yes" if value._data is not None else "No"])

        max_lengths = [max(len(str(row[i])) for row in table) for i in range(len(table[0]))]

        formatted_table = []
        for row in table:
            formatted_row = [str(cell).ljust(max_lengths[i]) for i, cell in enumerate(row)]
            formatted_table.append(formatted_row)

        table_str = ""
        for row in formatted_table:
            table_str += " | ".join(row) + "\n"

        return table_str

    def walk_data_streams(self) -> Generator[DataStream, None, None]:
        """Walk through the dataset tree and yield data streams."""
        for value in self.__dict__.values():
            if isinstance(value, DataStream):
                yield value
            elif isinstance(value, DataStreamGroup):
                yield from value.walk_data_streams()

    @staticmethod
    def group(data_streams: _GroupType) -> "DataStreamGroup":
        """Package data streams into a single data stream group."""
        if not isinstance(data_streams, dict):
            raise TypeError("data_streams must be a dictionary")
        return DataStreamGroup(
            io=DataStreamGroupBuilder(),
            reader_params=_UndefinedParams(),
            writer_params=_UndefinedParams(),
            read_on_init=False,
            _data_streams=data_streams,
        )
