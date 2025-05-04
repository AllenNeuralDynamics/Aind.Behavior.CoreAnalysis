import dataclasses
import json
import os
from typing import Generic, List, Optional, Type, TypeVar

import pandas as pd
import pydantic

from . import FilePathBaseParam
from ._core import DataStream


@dataclasses.dataclass
class JsonReaderParams:
    path: os.PathLike
    encoding: str = "UTF-8"


class Json(DataStream[dict[str, str], JsonReaderParams]):
    @staticmethod
    def _reader(params: JsonReaderParams) -> dict[str, str]:
        with open(params.path, "r", encoding=params.encoding) as file:
            data = json.load(file)
        return data

    parameters = JsonReaderParams


class MultiLineJson(DataStream[list[dict[str, str]], JsonReaderParams]):
    @staticmethod
    def _reader(params: JsonReaderParams) -> list[dict[str, str]]:
        with open(params.path, "r", encoding=params.encoding) as file:
            data = [json.loads(line) for line in file]
        return data

    parameters = JsonReaderParams


_TModel = TypeVar("_TModel", bound=pydantic.BaseModel)


@dataclasses.dataclass
class PydanticModelReaderParams(FilePathBaseParam, Generic[_TModel]):
    model: Type[_TModel]
    encoding: str = "UTF-8"


class PydanticModel(DataStream[_TModel, PydanticModelReaderParams[_TModel]]):
    @staticmethod
    def _reader(params: PydanticModelReaderParams[_TModel]) -> _TModel:
        with open(params.path, "r", encoding=params.encoding) as file:
            return params.model.model_validate_json(file.read())

    parameters = PydanticModelReaderParams


@dataclasses.dataclass
class MultiLinePydanticModelReaderParams(FilePathBaseParam, Generic[_TModel]):
    model: Type[_TModel]
    encoding: str = "UTF-8"
    index: Optional[str] = None
    column_names: Optional[dict[str, str]] = None


class MultiLinePydanticModel(DataStream[pd.DataFrame, MultiLinePydanticModelReaderParams[_TModel]]):
    @staticmethod
    def _reader(params: MultiLinePydanticModelReaderParams[_TModel]) -> pd.DataFrame:
        with open(params.path, "r", encoding=params.encoding) as file:
            model_ls = pd.DataFrame([params.model.model_validate_json(line).model_dump() for line in file])
        if params.column_names is not None:
            model_ls.rename(columns=params.column_names, inplace=True)
        if params.index is not None:
            model_ls.set_index(params.index, inplace=True)
        return model_ls

    parameters = MultiLinePydanticModelReaderParams


def multi_line_pydantic_model_reader(params: MultiLinePydanticModelReaderParams[_TModel]) -> List[_TModel]:
    with open(params.path, "r", encoding=params.encoding) as file:
        model_ls = [params.model.model_validate_json(line) for line in file]
    return model_ls
