import json
import os
import dataclasses
from typing import Callable, Generic, TypeVar, List
import pydantic
@dataclasses.dataclass
class JsonReaderParams:
    path: os.PathLike
    encoding: str = "UTF-8"


def json_reader(params: JsonReaderParams) -> dict[str, str]:
    with open(params.path, "r", encoding=params.encoding) as file:
        data = json.load(file)
    return data

@dataclasses.dataclass
class JsonWriterParams:
    path: os.PathLike
    indent: int = 4
    encoding: str = "UTF-8"


def json_writer(data: object, params: JsonWriterParams) -> None:
    with open(params.path, "w", encoding=params.encoding) as file:
        json.dump(data, file, indent=params.indent)
