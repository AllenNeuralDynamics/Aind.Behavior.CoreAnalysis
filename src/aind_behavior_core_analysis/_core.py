import abc
import dataclasses
import os
from typing import Any, Dict, Generator, Generic, List, Literal, Optional, Self, TypeVar, Union

from typing_extensions import override

from aind_behavior_core_analysis import _typing


def is_unset(obj: Any) -> bool:
    return (
        (obj is _typing.UnsetReader)
        or (obj is _typing.UnsetWriter)
        or (obj is _typing.UnsetParams)
        or (obj is _typing.UnsetData)
    )


class DataStream(Generic[_typing.TData, _typing.TReaderParams, _typing.TWriterParams]):
    def __init__(
        self: Self,
        name: str,
        description: Optional[str] = None,
        reader: _typing.IReader[_typing.TData, _typing.TReaderParams] = _typing.UnsetReader,
        writer: _typing.IWriter[_typing.TData, _typing.TWriterParams] = _typing.UnsetWriter,
        reader_params: _typing.TReaderParams = _typing.UnsetParams,
        writer_params: _typing.TWriterParams = _typing.UnsetParams,
        **kwargs: Any,
    ) -> None:
        self._name = name
        self._description = description
        self._reader: _typing.IReader[_typing.TData, _typing.TReaderParams] = reader
        self._writer: _typing.IWriter[_typing.TData, _typing.TWriterParams] = writer
        self._reader_params: _typing.TReaderParams = reader_params
        self._writer_params: _typing.TWriterParams = writer_params
        self._data: _typing.TData = _typing.UnsetData

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> Optional[str]:
        return self._description

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

    def at(self, key: str) -> Union["DataStream", "DataStreamGroup"]:
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

    def load(self) -> Self:
        """Load data into the data stream."""
        self._data = self.read()
        return self

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

    def __str__(self):
        return (
            f"DataStream("
            f"reader={self._reader}, "
            f"writer={self._writer}, "
            f"reader_params={self._reader_params}, "
            f"writer_params={self._writer_params}, "
            f"data_type={self._data.__class__.__name__ if self.has_data else 'Not Loaded'}"
        )


# Type hinting doesn't resolve subtypes of generics apparently.
# We pass the explicit, resolved, inner generics.
_StreamLike = Union[DataStream[Any, Any, Any], "DataStreamGroup[Any, Any, Any]", "StaticDataStreamGroup[Any, Any, Any]"]
StreamLikeCollection = TypeVar("StreamLikeCollection", bound=List[_StreamLike])


class DataStreamGroup(
    DataStream[StreamLikeCollection, _typing.TReaderParams, _typing.TWriterParams],
    Generic[StreamLikeCollection, _typing.TReaderParams, _typing.TWriterParams],
):
    def __init__(
        self,
        name: str,
        description: Optional[str] = None,
        reader: _typing.IReader[StreamLikeCollection, _typing.TReaderParams] = _typing.UnsetReader,
        writer: _typing.IWriter[StreamLikeCollection, _typing.TWriterParams] = _typing.UnsetWriter,
        reader_params: _typing.TReaderParams = _typing.UnsetParams,
        writer_params: _typing.TWriterParams = _typing.UnsetParams,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            name=name,
            description=description,
            reader=reader,
            writer=writer,
            reader_params=reader_params,
            writer_params=writer_params,
            **kwargs,
        )

        self._hashmap: Dict[str, _StreamLike] = {}
        self._update_hashmap()

    def _update_hashmap(self) -> None:
        """Validate the hashmap to ensure all keys are unique."""
        if not self.has_data:
            return
        stream_keys = [stream.name for stream in self.data_streams]
        duplicates = [name for name in stream_keys if stream_keys.count(name) > 1]
        if duplicates:
            raise ValueError(f"Duplicate names found in the data stream collection: {set(duplicates)}")
        self._hashmap = {stream.name: stream for stream in self.data_streams}
        return

    @property
    def data_streams(self) -> StreamLikeCollection:
        if not self.has_data:
            raise ValueError("Data has not been read yet.")
        return self._data

    @override
    def at(self, name: str) -> DataStream:
        """Get a data stream by key."""
        if not self.has_data:
            raise ValueError("data streams have not been read yet. Cannot access data streams.")
        if name in self._hashmap:
            return self._hashmap[name]
        else:
            raise KeyError(f"Stream with name: '{name}' not found in data streams.")

    def __str__(self):
        table = []
        table.append(["Stream Name", "Stream Type", "Is Loaded"])
        table.append(["-" * 20, "-" * 20, "-" * 20])

        if not self.has_data:
            return "DataStreamGroup has not been loaded yet."

        for key, value in self._hashmap.items():
            table.append(
                [key, value.data.__class__.__name__ if value.has_data else "Unknown", "Yes" if value.has_data else "No"]
            )

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
        for value in self.data_streams:
            if isinstance(value, DataStream):
                yield value
            if isinstance(value, DataStreamGroup):
                yield from value.walk_data_streams()


# Todo I think this could be made much easier by passing a "default_reader" that returns the data stream directly. For now I will leave it like this.
class StaticDataStreamGroup(DataStreamGroup[StreamLikeCollection, _typing.TReaderParams, _typing.TWriterParams]):
    def __init__(
        self,
        name: str,
        data_streams: StreamLikeCollection,
        description: Optional[str] = None,
        writer: _typing.IWriter[StreamLikeCollection, _typing.TWriterParams] = _typing.UnsetWriter,
        writer_params: _typing.TWriterParams = _typing.UnsetParams,
    ) -> None:
        """Initializes a special DataStreamGroup where the data streams are passed directly, without a reader."""
        super().__init__(
            name=name,
            description=description,
            reader=_typing.UnsetReader,
            writer=writer,
            reader_params=_typing.UnsetParams,
            writer_params=writer_params,
        )
        self.bind_data_streams(data_streams)

    def read(self) -> StreamLikeCollection:
        return self.data_streams

    def bind_data_streams(self, data_streams: StreamLikeCollection) -> Self:
        """Bind data streams to the data stream group."""
        if self.has_data:
            raise ValueError("Data streams are already set. Cannot bind again.")
        if not (is_unset(self._reader) and is_unset(self._writer)):
            raise ValueError("reader and writer must be unset to bind data streams directly.")
        self._data = data_streams
        self._update_hashmap()
        return self

    def add_stream(self, stream: _StreamLike) -> Self:
        """Add a new data stream to the group."""
        if not self.has_data:
            self._data: StreamLikeCollection = [stream]
            self._update_hashmap()
            return self

        if stream.name in self._hashmap:
            raise KeyError(f"Stream with name: '{stream.name}' already exists in data streams.")

        self.data_streams.append(stream)
        self._update_hashmap()
        return self

    def remove_stream(self, name: str) -> None:
        """Remove a data stream from the group."""

        if not self.has_data:
            raise ValueError("Data streams have not been read yet. Cannot access data streams.")

        if name not in self._hashmap:
            raise KeyError(f"Data stream with name '{name}' not found in data streams.")
        self.data_streams.remove(self._hashmap[name])
        self._update_hashmap()
        return


@dataclasses.dataclass
class Dataset:
    name: str
    version: str
    description: str
    data_streams: DataStreamGroup

    def tree(self, exclude_params: bool = False, print_if_none: bool = False) -> str:
        return print_data_stream_tree(self.data_streams, exclude_params=exclude_params, print_if_none=print_if_none)


def print_data_stream_tree(
    node: DataStreamGroup | DataStream, prefix="", *, exclude_params: bool = False, print_if_none: bool = False
) -> str:
    icon_map = {
        DataStream: "📄",
        DataStreamGroup: "📂",
        StaticDataStreamGroup: "🧊",
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
        params_str = str(params) if reader_or_writer else ""
        _str = f"{prefix}{icon_map[io_type]} {io_name}\n"
        if not exclude_params:
            _str += f"{prefix}    <{params_str}>\n"
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

    if isinstance(node, DataStreamGroup) or isinstance(node, StaticDataStreamGroup):
        if not node.has_data:
            s_builder += f"\n{prefix}{icon_map[None]} Not loaded"
        else:
            for key, child in node.data_streams.items():
                s_builder += f"\n{prefix}{icon_map[type(child)]} {key}"
                child_tree = print_data_stream_tree(
                    child, prefix + "    ", exclude_params=exclude_params, print_if_none=print_if_none
                )
                if child_tree:
                    s_builder += f"\n{child_tree}"

    return s_builder.strip()


@dataclasses.dataclass
class FilePathBaseParam(abc.ABC):
    path: os.PathLike
