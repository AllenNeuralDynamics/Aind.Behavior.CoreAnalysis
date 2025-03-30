import os
from typing import Literal, Optional

import pandas as pd
from pydantic import BaseModel, Field

from .base import DataStream, DataStreamBuilder
from .core import FileReaderParams

class FileReaderParams(BaseModel):
    path: os.PathLike = Field(description="Path to the file")


class CsvReaderParams(FileReaderParams):
    delimiter: Optional[str] = Field(default=None, description="Delimiter used in the CSV file")
    strict_header: bool = Field(default=True, description="Whether to raise an error if the header is not found")


def csv_reader(params: CsvReaderParams) -> pd.DataFrame:
    return pd.read_csv(params.path, delimiter=params.delimiter, header=0 if params.strict_header else None)


class _FileWriterParams(BaseModel):
    path: os.PathLike = Field(description="Path to the file")


class CsvWriterParams(_FileWriterParams):
    delimiter: str = Field(default=",", description="Delimiter used in the CSV file")
    encoding: Optional[Literal["utf-8"]] = Field(default=None, description="Encoding used in the CSV file")


def csv_writer(data: pd.DataFrame, params: CsvWriterParams) -> None:
    data.to_csv(params.path, sep=params.delimiter, index=False, encoding=params.encoding)


CsvBuilder = DataStreamBuilder(reader=csv_reader, writer=csv_writer)

