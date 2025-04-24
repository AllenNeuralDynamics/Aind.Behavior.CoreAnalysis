from dataclasses import dataclass

from . import FilePathBaseParam


@dataclass
class TextReaderParams(FilePathBaseParam):
    encoding: str = "UTF-8"


def text_reader(params: TextReaderParams) -> str:
    with open(params.path, "r", encoding=params.encoding) as file:
        return file.read()
