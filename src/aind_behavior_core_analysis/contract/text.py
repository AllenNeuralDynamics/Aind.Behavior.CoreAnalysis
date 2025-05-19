from dataclasses import dataclass

from ._core import DataStream, FilePathBaseParam


@dataclass
class TextParams(FilePathBaseParam):
    encoding: str = "UTF-8"


class Text(DataStream[str, TextParams]):
    @staticmethod
    def _reader(params: TextParams) -> str:
        with open(params.path, "r", encoding=params.encoding) as file:
            return file.read()

    make_params = TextParams
