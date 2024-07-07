from __future__ import annotations

import json
from os import PathLike
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
        name: Optional[str] = None,
        auto_load: bool = False,
        inner_parser: Optional[TInnerParser] = None,
    ) -> None:
        super().__init__(path, name, auto_load)
        self._inner_parser: Optional[TInnerParser] = inner_parser

    def load(self, path: Optional[PathLike] = None, force_reload: bool = False) -> DataFrameOrSeries:

        super().load(path, force_reload)

        self._data = self._apply_inner_parser(self._data)
        return self._data

    @classmethod
    def _file_reader(cls, path) -> List[str]:
        with open(path, "r+", encoding="utf-8") as f:
            return f.readlines()

    @classmethod
    def _reader(
        cls,
        value: List[str],
        validate: bool = True,
        pydantic_validate_kwargs: Optional[dict] = None,
        pydantic_model_dump_kwargs: Optional[dict] = None,
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
    def _parser(cls, value) -> DataFrameOrSeries:
        return cls._reader(value)

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
