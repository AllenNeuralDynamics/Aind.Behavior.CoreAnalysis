from dataclasses import dataclass

from . import FilePathBaseParam
from ._core import DataStream


@dataclass
class TextReaderParams(FilePathBaseParam):
    encoding: str = "UTF-8"


class Text(DataStream[str, TextReaderParams]):
    @staticmethod
    def _reader(params: TextReaderParams) -> str:
        with open(params.path, "r", encoding=params.encoding) as file:
            return file.read()

    make_params = TextReaderParams
