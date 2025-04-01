import dataclasses
from typing import Any, Dict, Generator, Generic, Literal, Optional, Protocol, Self, TypeVar, Union, final

from pydantic import BaseModel

_TData = TypeVar("_TData", bound=Any)

_TReaderParams = TypeVar("_TReaderParams", bound=BaseModel, contravariant=True)
_TWriterParams = TypeVar("_TWriterParams", bound=BaseModel, contravariant=True)
_co_TData = TypeVar("_co_TData", covariant=True, bound=Any)
_contra_TData = TypeVar("_contra_TData", contravariant=True, bound=Any)


class _Reader(Protocol, Generic[_co_TData, _TReaderParams]):
    def __call__(self, params: _TReaderParams) -> _co_TData: ...


class _Writer(Protocol, Generic[_contra_TData, _TWriterParams]):
    def __call__(self, data: _contra_TData, params: _TWriterParams) -> Any: ...


class _UnsetReader(_Reader[_TData, _TReaderParams]):
    def __call__(self, params: Any) -> Any:
        raise NotImplementedError("Reader is not set.")


class _UnsetWriter(_Writer[_TData, _TWriterParams]):
    def __call__(self, data: Any, params: Any) -> None:
        raise NotImplementedError("Writer is not set.")


def is_unset(obj: Any) -> bool:
    return isinstance(obj, (_UnsetReader, _UnsetWriter))


class DataStream(Generic[_TData, _TReaderParams, _TWriterParams]):
    def __init__(
        self: Self,
        reader: _Reader[_TData, _TReaderParams] = _UnsetReader(),
        writer: _Writer[_TData, _TWriterParams] = _UnsetWriter(),
        reader_params: Optional[_TReaderParams] = None,
        writer_params: Optional[_TWriterParams] = None,
        read_on_init: bool = False,
        **kwargs: Any,
    ) -> None:
        self._reader: _Reader[_TData, _TReaderParams] = reader
        self._writer: _Writer[_TData, _TWriterParams] = writer
        self._reader_params: Optional[_TReaderParams] = reader_params
        self._writer_params: Optional[_TWriterParams] = writer_params
        self._data: Optional[_TData] = None
        if read_on_init:
            self.load()

    @property
    def reader(self) -> _Reader[_TData, _TReaderParams]:
        if self._reader is None:
            raise ValueError("Reader is not set.")
        return self._reader

    @property
    def writer(self) -> _Writer[_TData, _TWriterParams]:
        if self._writer is None:
            raise ValueError("Writer is not set.")
        return self._writer

    def bind_reader_params(self, params: _TReaderParams) -> Self:
        """Bind reader parameters to the data stream."""
        if is_unset(self._reader):
            raise ValueError("Reader is not set. Cannot bind parameters.")
        if self._reader_params is not None:
            raise ValueError("Reader parameters are already set. Cannot bind again.")
        self._reader_params = params
        return self

    def bind_writer_params(self, params: _TWriterParams) -> Self:
        """Bind writer parameters to the data stream."""
        if is_unset(self._writer):
            raise ValueError("Writer is not set. Cannot bind parameters.")
        if self._writer_params is not None:
            raise ValueError("Writer parameters are already set. Cannot bind again.")
        self._writer_params = params
        return self

    @property
    def has_data(self) -> bool:
        """Check if the data stream has data."""
        return self._data is not None

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
        if is_unset(self._reader):
            raise ValueError("Reader is not set. Cannot read data.")
        if self._reader_params is None:
            raise ValueError("Reader parameters are not set. Cannot read data.")
        return self._reader(self._reader_params)

    def write(self, data: Optional[_TData] = None) -> None:
        """Write data to the data stream."""
        if is_unset(self._writer):
            raise ValueError("Writer is not set. Cannot write data.")
        if self._writer_params is None:
            raise ValueError("Writer parameters are not set. Cannot write data.")
        if data is None:
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


@final
class _UndefinedParams(BaseModel):
    pass


class DataStreamGroup(DataStream[KeyedStreamLike, _TReaderParams, _TWriterParams]):
    @property
    def data_streams(self) -> KeyedStreamLike:
        if self._data is None:
            raise ValueError("Data has not been read yet.")
        return self._data

    def bind_data_streams(self, data_streams: KeyedStreamLike) -> Self:
        """Bind data streams to the data stream group."""
        if self._data is not None:
            raise ValueError("Data streams are already set. Cannot bind again.")
        if not (is_unset(self._reader) and is_unset(self._writer)):
            raise ValueError("reader and writer must be unset to bind data streams directly.")
        self._data = data_streams
        return self

    def __getitem__(self, key: str) -> Union[DataStream, "DataStreamGroup"]:
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
        """Walk through the dataset tree and yield data streams."""
        for value in self.data_streams.values():
            if isinstance(value, DataStream):
                yield value
            elif isinstance(value, DataStreamGroup):
                yield from value.walk_data_streams()

    @staticmethod
    def group(data_streams: KeyedStreamLike) -> "DataStreamGroup[KeyedStreamLike, _UndefinedParams, _UndefinedParams]":
        """Package data streams into a single data stream group."""
        return DataStreamGroup[KeyedStreamLike, _UndefinedParams, _UndefinedParams](
            reader=_UnsetReader(),
            writer=_UnsetWriter(),
            reader_params=None,
            writer_params=None,
            read_on_init=False,
        ).bind_data_streams(data_streams)


@dataclasses.dataclass
class Dataset:
    name: str
    version: str
    description: str
    data_streams: DataStreamGroup

    def print(self, exclude_params: bool = False, print_if_none: bool = False) -> None:
        """
        Prints the structure of the dataset using emojis.
        :param exclude_params: If True, excludes the parameters from the output.
        :param print_if_none: If True, prints "None" for None values.
        """
        print_data_stream_tree(self.data_streams, exclude_params=exclude_params, print_if_none=print_if_none)


def print_data_stream_tree(
    node: DataStreamGroup | DataStream, prefix="", *, exclude_params: bool = False, print_if_none: bool = False
) -> None:
    """
    Prints the structure of the data streams using emojis.
    :param node: The current node (DatasetNode or DataStream).
    :param prefix: The prefix for the tree structure.
    """

    icon_map = {
        DataStream: "📄",
        DataStreamGroup: "📂",
        None: "❓",
        "reader": "⬇️",
        "writer": "⬆️",
    }

    def _print_io(
        reader_or_writer,
        params,
        prefix: str,
        io_type: Literal["reader", "writer"],
        *,
        print_if_none: bool = False,
        exclude_params: bool = False,
    ) -> str:
        if not print_if_none and is_unset(reader_or_writer):
            return ""
        io_name = reader_or_writer.__name__ if reader_or_writer else "None"
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
        print_if_none=print_if_none,
        exclude_params=exclude_params,
    )
    s_builder += _print_io(
        node._writer,
        node._writer_params,
        prefix,
        "writer",
        print_if_none=print_if_none,
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
