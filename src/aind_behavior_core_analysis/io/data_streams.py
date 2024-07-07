from __future__ import annotations
import csv
import json
from os import PathLike
from pathlib import Path
import io
from typing import (
    Any,
    Callable,
    Generic,
    List,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
from aind_behavior_services.data_types import RenderSynchState, SoftwareEvent
from pydantic import BaseModel, TypeAdapter

from aind_behavior_core_analysis.io._core import (
    DataStream,
    DataStreamSource,
    StreamCollection,
    TData,
)

DataFrameOrSeries = Union[pd.DataFrame, pd.Series]

TInnerParser = TypeVar("TInnerParser", None, BaseModel)


class SoftwareEventStream(DataStream[DataFrameOrSeries], Generic[TInnerParser]):
    """Represents a generic Software event."""

    def __init__(
        self,
        path: Optional[PathLike],
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        inner_parser: Optional[TInnerParser] = None,
        **kwargs
    ) -> None:
        super().__init__(path, name=name, auto_load=auto_load, **kwargs)
        self._inner_parser: Optional[TInnerParser] = inner_parser

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
        **kwargs
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

    @classmethod
    def _parser(cls, value, *args, **kwargs) -> DataFrameOrSeries:
        return cls._reader(value, *args, **kwargs)

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
        self,
        path: Optional[PathLike],
        *,
        name: Optional[str] = None,
        auto_load: bool = False,
        **kwargs
    ) -> None:
        super().__init__(path, name=name, auto_load=auto_load, **kwargs)

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
        **kwargs
    ) -> DataFrameOrSeries:

        has_header = csv.Sniffer().has_header(value)
        _header = 0 if has_header is True else None
        df = pd.read_csv(
            io.StringIO(value),
            header=_header,
            index_col=infer_index_col,
            names=col_names)
        return df

    @classmethod
    def _parser(cls, value, *args, **kwargs) -> DataFrameOrSeries:
        return cls._reader(value, *args, **kwargs)