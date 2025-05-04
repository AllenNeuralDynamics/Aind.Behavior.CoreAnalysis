import abc
import dataclasses
import os
from typing import Any, Dict, Generator, Generic, List, Optional, ParamSpec, Self, TypeVar

from typing_extensions import override

from aind_behavior_core_analysis import _typing


def is_unset(obj: Any) -> bool:
    return (obj is _typing.UnsetReader) or (obj is _typing.UnsetParams) or (obj is _typing.UnsetData)


P = ParamSpec("P")


class DataStream(abc.ABC, Generic[_typing.TData, _typing.TReaderParams]):
    def __init__(
        self: Self,
        name: str,
        *,
        description: Optional[str] = None,
        reader_params: Optional[_typing.TReaderParams] = None,
        **kwargs,
    ) -> None:
        self._name = name
        self._description = description
        self._reader_params = reader_params if reader_params is not None else _typing.UnsetParams
        self._data = _typing.UnsetData

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> Optional[str]:
        return self._description

    _reader: _typing.IReader[_typing.TData, _typing.TReaderParams] = _typing.UnsetReader

    make_params = NotImplementedError("make_params is not implemented for DataStream.")

    def read(self, reader_params: Optional[_typing.TReaderParams] = None) -> _typing.TData:
        reader_params = reader_params if reader_params is not None else self._reader_params
        if is_unset(reader_params):
            raise ValueError("Reader parameters are not set. Cannot read data.")
        return self._reader(reader_params)

    def bind_reader_params(self, params: _typing.TReaderParams) -> Self:
        """Bind reader parameters to the data stream."""
        if not is_unset(self._reader_params):
            raise ValueError("Reader parameters are already set. Cannot bind again.")
        self._reader_params = params
        return self

    def at(self, name: str) -> "DataStream":
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

    def __str__(self):
        return (
            f"DataStream("
            f"stream_type={self.__class__.__name__}, "
            f"name={self._name}, "
            f"description={self._description}, "
            f"reader_params={self._reader_params}, "
            f"data_type={self._data.__class__.__name__ if self.has_data else 'Not Loaded'}"
        )


# Type hinting doesn't resolve subtypes of generics apparently.
# We pass the explicit, resolved, inner generics.
TDataStream = TypeVar("TDataStream", bound=DataStream[Any, Any])


class DataStreamCollectionBase(
    DataStream[List[TDataStream], _typing.TReaderParams],
    Generic[TDataStream, _typing.TReaderParams],
):
    def __init__(
        self: Self,
        name: str,
        *,
        description: Optional[str] = None,
        reader_params: Optional[_typing.TReaderParams] = None,
        **kwargs,
    ) -> None:
        super().__init__(name=name, description=description, reader_params=reader_params, **kwargs)
        self._hashmap: Dict[str, TDataStream] = {}
        self._update_hashmap()

    def _update_hashmap(self) -> None:
        """Validate the hashmap to ensure all keys are unique."""
        if not self.has_data:
            return
        stream_keys = [stream.name for stream in self.data]
        duplicates = [name for name in stream_keys if stream_keys.count(name) > 1]
        if duplicates:
            raise ValueError(f"Duplicate names found in the data stream collection: {set(duplicates)}")
        self._hashmap = {stream.name: stream for stream in self.data}
        return

    @override
    def load(self):
        super().load()
        if not isinstance(self._data, list):
            self._data = _typing.UnsetData
            raise ValueError("Data must be a list of DataStreams.")
        self._update_hashmap()
        return self

    @override
    def at(self, name: str) -> TDataStream:
        """Get a data stream by key."""
        if not self.has_data:
            raise ValueError("data streams have not been read yet. Cannot access data streams.")
        if name in self._hashmap:
            return self._hashmap[name]
        else:
            raise KeyError(f"Stream with name: '{name}' not found in data streams.")

    def __str__(self: Self) -> str:
        table = []
        table.append(["Stream Name", "Stream Type", "Is Loaded"])
        table.append(["-" * 20, "-" * 20, "-" * 20])

        if not self.has_data:
            return f"{self.__class__.__name__} has not been loaded yet."

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
        for value in self._hashmap.values():
            if isinstance(value, DataStream):
                yield value
            if isinstance(value, DataStreamCollectionBase):
                yield from value.walk_data_streams()


class DataStreamCollection(DataStreamCollectionBase[DataStream, _typing.UnsetParamsType]):
    @override
    def __init__(
        self,
        name: str,
        data_streams: List[DataStream],
        *,
        description: Optional[str] = None,
    ) -> None:
        """Initializes a special DataStreamGroup where the data streams are passed directly, without a reader."""
        super().__init__(
            name=name,
            description=description,
            reader_params=_typing.UnsetParams,
        )
        self.bind_data_streams(data_streams)

    @staticmethod
    def parameters(*args, **kwargs) -> _typing.UnsetParamsType:
        """Parameters function to return UnsetParams."""
        return _typing.UnsetParams

    def _reader(self, *args, **kwargs) -> List[DataStream]:
        """Reader function to read data from the generator."""
        return self._data

    def bind_data_streams(self, data_streams: List[DataStream]) -> Self:
        """Bind data streams to the data stream group."""
        if self.has_data:
            raise ValueError("Data streams are already set. Cannot bind again.")
        self._data = data_streams
        self._update_hashmap()
        return self

    def add_stream(self, stream: DataStream) -> Self:
        """Add a new data stream to the group."""
        if not self.has_data:
            self._data = [stream]
            self._update_hashmap()
            return self

        if stream.name in self._hashmap:
            raise KeyError(f"Stream with name: '{stream.name}' already exists in data streams.")

        self._data.append(stream)
        self._update_hashmap()
        return self

    def remove_stream(self, name: str) -> None:
        """Remove a data stream from the group."""

        if not self.has_data:
            raise ValueError("Data streams have not been read yet. Cannot access data streams.")

        if name not in self._hashmap:
            raise KeyError(f"Data stream with name '{name}' not found in data streams.")
        self._data.remove(self._hashmap[name])
        self._update_hashmap()
        return


@dataclasses.dataclass
class Dataset:
    name: str
    version: str
    description: str
    data_streams: DataStream

    def tree(self, exclude_params: bool = False, print_if_none: bool = False) -> str:
        return print_data_stream_tree(self.data_streams, exclude_params=exclude_params, print_if_none=print_if_none)


def print_data_stream_tree(
    node: DataStream, prefix="", *, exclude_params: bool = False, print_if_none: bool = False
) -> str:
    icon_map = {
        DataStream: "📄",
        DataStreamCollectionBase: "📂",
        DataStreamCollection: "🧊",
        None: "❓",
        _typing.UnsetParams: "❓",
        _typing.UnsetReader: "❓",
        _typing.UnsetData: "❓",
    }

    s_builder = ""
    s_builder = s_builder.rstrip("\n")

    if isinstance(node, DataStreamCollectionBase):
        if not node.has_data:
            s_builder += f"\n{prefix}{icon_map[None]} Not loaded"
        else:
            for child in node.data:
                s_builder += f"\n{prefix}{icon_map[type(child)]} {child.name}"
                child_tree = print_data_stream_tree(
                    child, prefix + "    ", exclude_params=exclude_params, print_if_none=print_if_none
                )
                if child_tree:
                    s_builder += f"\n{child_tree}"

    return s_builder.strip()


@dataclasses.dataclass
class FilePathBaseParam(abc.ABC):
    path: os.PathLike
