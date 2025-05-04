import dataclasses
import json
import os
from typing import Generic, Optional, Type, TypeVar

import pandas as pd
import pydantic

from . import FilePathBaseParam
from ._core import DataStream


@dataclasses.dataclass
class JsonParams:
    path: os.PathLike
    encoding: str = "UTF-8"


class Json(DataStream[dict[str, str], JsonParams]):
    @staticmethod
    def _reader(params: JsonParams) -> dict[str, str]:
        with open(params.path, "r", encoding=params.encoding) as file:
            data = json.load(file)
        return data

    make_params = JsonParams


class MultiLineJson(DataStream[list[dict[str, str]], JsonParams]):
    @staticmethod
    def _reader(params: JsonParams) -> list[dict[str, str]]:
        with open(params.path, "r", encoding=params.encoding) as file:
            data = [json.loads(line) for line in file]
        return data

    make_params = JsonParams


_TModel = TypeVar("_TModel", bound=pydantic.BaseModel)


@dataclasses.dataclass
class PydanticModelParams(FilePathBaseParam, Generic[_TModel]):
    model: Type[_TModel]
    encoding: str = "UTF-8"


class PydanticModel(DataStream[_TModel, PydanticModelParams[_TModel]]):
    @staticmethod
    def _reader(params: PydanticModelParams[_TModel]) -> _TModel:
        with open(params.path, "r", encoding=params.encoding) as file:
            return params.model.model_validate_json(file.read())

    make_params = PydanticModelParams


@dataclasses.dataclass
class ManyPydanticModelParams(FilePathBaseParam, Generic[_TModel]):
    model: Type[_TModel]
    encoding: str = "UTF-8"
    index: Optional[str] = None
    column_names: Optional[dict[str, str]] = None


class ManyPydanticModel(DataStream[pd.DataFrame, ManyPydanticModelParams[_TModel]]):
    @staticmethod
    def _reader(params: ManyPydanticModelParams[_TModel]) -> pd.DataFrame:
        with open(params.path, "r", encoding=params.encoding) as file:
            model_ls = pd.DataFrame([params.model.model_validate_json(line).model_dump() for line in file])
        if params.column_names is not None:
            model_ls.rename(columns=params.column_names, inplace=True)
        if params.index is not None:
            model_ls.set_index(params.index, inplace=True)
        return model_ls

    make_params = ManyPydanticModelParams
