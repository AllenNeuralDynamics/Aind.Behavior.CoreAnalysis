from __future__ import annotations

import abc
from collections import UserDict
from os import PathLike
from pathlib import Path
from typing import (
    Any,
    Generic,
    Optional,
    Self,
    TypeVar,
)

TData = TypeVar("TData", bound=Any)


class DataStream(abc.ABC, Generic[TData]):
    _data: Optional[TData]
    _path: Optional[Path]
    _name: str
    _auto_load: bool

    def __init__(
        self,
        /,
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

    @classmethod
    @abc.abstractmethod
    def _file_reader(cls, path: PathLike, *args, **kwargs) -> Any:
        pass

    @abc.abstractmethod
    def _parser(self, *args, **kwargs) -> TData:
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

    def load(self) -> TData:
        return self._load()

    def reload(self) -> TData:
        return self._load(force_reload=True)

    def from_file(self, path: PathLike) -> Any:
        return self._parser(self._file_reader(path))

    def _load(self, /, path: Optional[PathLike] = None, *, force_reload: bool = False, **kwargs) -> TData:
        if force_reload is False and self._data:
            pass
        else:
            path = Path(path) if path is not None else self.path
            if path:
                self._path = path
                self._data = self._parser(self._file_reader(path))
            else:
                raise ValueError("Path attribute is not defined. Cannot load data.")
        return self._data

    def __str__(self) -> str:
        return f"{self.__class__.__name__} stream with data{'' if self._data is not None else 'not'} loaded."


class DataStreamCollectionFactory(abc.ABC):
    @abc.abstractmethod
    def build(self) -> DataStreamCollection: ...


class DataStreamCollection(UserDict[str, DataStream]):
    """Represents a collection of data streams."""

    def __str__(self):
        table = []
        table.append(["Stream Name", "Stream Type", "Is Loaded"])
        table.append(["-" * 20, "-" * 20, "-" * 20])
        for key, value in self.items():
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

    def try_append(self, key: str, value: DataStream) -> Self:
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
        return self

    def merge(self, *others: DataStreamCollection) -> Self:
        for other in others:
            for key, value in other.items():
                _ = self.try_append(key, value)
        return self

    def reload_streams(self) -> None:
        for stream in self.values():
            stream.reload()

    def load_streams(self) -> None:
        for stream in self.values():
            stream.load()

    @classmethod
    def from_merge(cls, *others: DataStreamCollection) -> DataStreamCollection:
        return cls().merge(*others)
