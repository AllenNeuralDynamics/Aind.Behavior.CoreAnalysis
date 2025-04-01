import json

from pydantic import Field

from aind_behavior_core_analysis.base import FilePathParams


class JsonReaderParams(FilePathParams):
    encoding: str = Field(default="UTF-8", description="Encoding used in the JSON file")


def json_reader(params: JsonReaderParams) -> dict[str, str]:
    with open(params.path, "r", encoding=params.encoding) as file:
        data = json.load(file)
    return data


class JsonWriterParams(FilePathParams):
    indent: int = Field(default=4, description="Indentation level for JSON file")
    encoding: str = Field(default="UTF-8", description="Encoding used in the JSON file")


def json_writer(data: object, params: JsonWriterParams) -> None:
    with open(params.path, "w", encoding=params.encoding) as file:
        json.dump(data, file, indent=params.indent)
