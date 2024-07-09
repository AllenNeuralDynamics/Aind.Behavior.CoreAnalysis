from __future__ import annotations
import csv
import io
import json
from os import PathLike
from pathlib import Path
from typing import (
    List,
    Optional,
    Union,
)

import harp
import pandas as pd
from aind_behavior_services.data_types import SoftwareEvent
from harp.reader import RegisterReader
from pydantic import BaseModel
from typing_extensions import override

from aind_behavior_core_analysis.io._core import (
    DataStream,
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


class HarpRegisterStream(DataStream[DataFrameOrSeries]):

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
