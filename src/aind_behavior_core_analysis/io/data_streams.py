from __future__ import annotations

import json
from typing import List, Optional, Union
import pandas as pd

from aind_behavior_services.data_types import RenderSynchState, SoftwareEvent

from aind_behavior_core_analysis.io._core import (
    DataStream,
    DataStreamSource,
    StreamCollection,
)

DataFrameOrSeries = Union[pd.DataFrame, pd.Series]


class SoftwareEventStream(DataStream[DataFrameOrSeries]):
    """Represents a generic Software event."""

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
            _objects = [
                SoftwareEvent.model_validate_json(
                    line, **(pydantic_validate_kwargs if pydantic_validate_kwargs else {})
                ).model_dump(**(pydantic_model_dump_kwargs if pydantic_model_dump_kwargs else {}))
                for line in value
            ]
        else:
            _objects = [json.loads(line) for line in value]

        df = pd.DataFrame(_objects)
        df.set_index("timestamp", inplace=True)
        return df

    @classmethod
    def _parser(cls, value) -> DataFrameOrSeries:
        return cls._reader(value)
