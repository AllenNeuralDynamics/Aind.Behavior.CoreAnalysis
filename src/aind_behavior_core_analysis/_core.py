import abc
import dataclasses
import os
from typing import Any, Dict, Generator, Generic, Literal, Self, TypeVar, Union

from typing_extensions import override

from aind_behavior_core_analysis import _typing


def is_unset(obj: Any) -> bool:
    return (
        (obj is _typing.UnsetReader)
        or (obj is _typing.UnsetWriter)
        or (obj is _typing.UnsetParams)
        or (obj is _typing.UnsetData)
        or (obj is _typing.NullParams)
    )


class DataStream(Generic[_typing.TData, _typing.TReaderParams, _typing.TWriterParams]):
    def __init__(
        self: Self,
        reader: _typing.IReader[_typing.TData, _typing.TReaderParams] = _typing.UnsetReader,
        writer: _typing.IWriter[_typing.TData, _typing.TWriterParams] = _typing.UnsetWriter,
        reader_params: _typing.TReaderParams = _typing.UnsetParams,
        writer_params: _typing.TWriterParams = _typing.UnsetParams,
        **kwargs: Any,
    ) -> None:
        self._reader: _typing.IReader[_typing.TData, _typing.TReaderParams] = reader
        self._writer: _typing.IWriter[_typing.TData, _typing.TWriterParams] = writer
        self._reader_params: _typing.TReaderParams = reader_params
        self._writer_params: _typing.TWriterParams = writer_params
        self._data: _typing.TData = _typing.UnsetData

    @property
    def reader(self) -> _typing.IReader[_typing.TData, _typing.TReaderParams]:
        if is_unset(self._reader):
            raise ValueError("Reader is not set.")
        return self._reader

    @property
    def writer(self) -> _typing.IWriter[_typing.TData, _typing.TWriterParams]:
        if is_unset(self._writer):
            raise ValueError("Writer is not set.")
        return self._writer

    def bind_reader_params(self, params: _typing.TReaderParams) -> Self:
        """Bind reader parameters to the data stream."""
        if is_unset(self._reader):
            raise ValueError("Reader is not set. Cannot bind parameters.")
        if not is_unset(self._reader_params):
            raise ValueError("Reader parameters are already set. Cannot bind again.")
        self._reader_params = params
        return self

    def bind_writer_params(self, params: _typing.TWriterParams) -> Self:
        """Bind writer parameters to the data stream."""
        if is_unset(self._writer):
            raise ValueError("Writer is not set. Cannot bind parameters.")
        if not is_unset(self._writer_params):
            raise ValueError("Writer parameters are already set. Cannot bind again.")
        self._writer_params = params
        return self

    def get_stream(self, key: str) -> Union["DataStream", "DataStreamGroup"]:
        """Get a data stream by key."""
        raise NotImplementedError("This method is not implemented for DataStream.")

    @property
    def has_data(self) -> bool:
        """Check if the data stream has data."""
        return not is_unset(self._data)

    @property
    def data(self) -> _typing.TData:
        if not self.has_data:
            raise ValueError("Data has not been loaded yet.")
        return self._data

    def load(self) -> None:
        """Load data into the data stream."""
        self._data = self.read()

    def read(self) -> _typing.TData:
        """Read data from the data stream."""
        if is_unset(self._reader):
            raise ValueError("Reader is not set. Cannot read data.")
        if is_unset(self._reader_params):
            raise ValueError("Reader parameters are not set. Cannot read data.")
        return self._reader(self._reader_params)

    def write(self, data: _typing.TData = _typing.UnsetData) -> None:
        """Write data to the data stream."""
        if is_unset(self._writer):
            raise ValueError("Writer is not set. Cannot write data.")
        if is_unset(self._writer_params):
            raise ValueError("Writer parameters are not set. Cannot write data.")
        if is_unset(data):
            data = self.data
        self._writer(data, self._writer_params)


# Type hinting doesn't resolve subtypes of generics apparently.
# We pass the explicit, resolved, inner generics.
KeyedStreamLike = TypeVar(
    "KeyedStreamLike",
    bound=Union[
        Dict[str, Union[DataStream[Any, Any, Any], "DataStreamGroup[Any, Any, Any]"]],
        Dict[str, "DataStreamGroup[Any, Any, Any]"],
        Dict[str, Union[DataStream[Any, Any, Any]]],
    ],
)


class DataStreamGroup(DataStream[KeyedStreamLike, _typing.TReaderParams, _typing.TWriterParams]):
    @property
    def data_streams(self) -> KeyedStreamLike:
        if self._data is None:
            raise ValueError("Data has not been read yet.")
        return self._data

    def bind_data_streams(self, data_streams: KeyedStreamLike) -> Self:
        """Bind data streams to the data stream group."""
        if self.has_data:
            raise ValueError("Data streams are already set. Cannot bind again.")
        if not (is_unset(self._reader) and is_unset(self._writer)):
            raise ValueError("reader and writer must be unset to bind data streams directly.")
        self._data = data_streams
        return self

    @override
    def get_stream(self, key: str) -> Union[DataStream, "DataStreamGroup"]:
        """Get a data stream by key."""
        if not self.has_data:
            raise ValueError("data streams have not been read yet. Cannot access data streams.")
        if key in self.data_streams:
            return self.data_streams[key]
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
        for value in self.data_streams.values():
            if isinstance(value, DataStream):
                yield value
            elif isinstance(value, DataStreamGroup):
                yield from value.walk_data_streams()

    @staticmethod
    def group(
        data_streams: KeyedStreamLike,
    ) -> "DataStreamGroup[KeyedStreamLike, _typing.NullParams, _typing.NullParams]":
        return DataStreamGroup[KeyedStreamLike, _typing.NullParams, _typing.NullParams](
            reader=_typing.UnsetReader,
            writer=_typing.UnsetWriter,
            reader_params=_typing.UnsetParams,
            writer_params=_typing.UnsetParams,
        ).bind_data_streams(data_streams)


@dataclasses.dataclass
class Dataset:
    name: str
    version: str
    description: str
    data_streams: DataStreamGroup

    def print(self, exclude_params: bool = False, print_if_none: bool = False) -> None:
        print_data_stream_tree(self.data_streams, exclude_params=exclude_params, print_if_none=print_if_none)


def print_data_stream_tree(
    node: DataStreamGroup | DataStream, prefix="", *, exclude_params: bool = False, print_if_none: bool = False
) -> None:
    icon_map = {
        DataStream: "📄",
        DataStreamGroup: "📂",
        None: "❓",
        _typing.UnsetParams: "❓",
        _typing.UnsetReader: "❓",
        _typing.UnsetWriter: "❓",
        _typing.UnsetData: "❓",
        "reader": "⬇️",
        "writer": "⬆️",
    }

    def _print_io(
        reader_or_writer,
        params,
        prefix: str,
        io_type: Literal["reader", "writer"],
        *,
        print_if_unset: bool = False,
        exclude_params: bool = False,
    ) -> str:
        if not print_if_unset and is_unset(reader_or_writer):
            return ""
        io_name = reader_or_writer.__name__ if reader_or_writer else "Unset"
        params = params if reader_or_writer else ""
        _str = f"{prefix}{icon_map[io_type]}{io_name} \n"
        if not exclude_params:
            _str += f"{prefix}   <{params}>\n"
        return _str

    s_builder = ""
    s_builder += _print_io(
        node._reader,
        node._reader_params,
        prefix,
        "reader",
        print_if_unset=print_if_none,
        exclude_params=exclude_params,
    )
    s_builder += _print_io(
        node._writer,
        node._writer_params,
        prefix,
        "writer",
        print_if_unset=print_if_none,
        exclude_params=exclude_params,
    )
    s_builder = s_builder.rstrip("\n")
    print(s_builder) if s_builder else None

    if isinstance(node, DataStreamGroup):
        if not node.has_data:
            print(f"{prefix}{prefix}{icon_map[None]} Not loaded")
        else:
            for key, child in node.data_streams.items():
                print(f"{prefix}{icon_map[type(child)]} {key}")
                print_data_stream_tree(child, prefix + "    ", exclude_params=exclude_params)


@dataclasses.dataclass
class FilePathBaseParam(abc.ABC):
    path: os.PathLike
