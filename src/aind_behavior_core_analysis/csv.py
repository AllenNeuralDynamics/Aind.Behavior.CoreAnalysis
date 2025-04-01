from typing import Literal, Optional

import pandas as pd
from pydantic import Field

from aind_behavior_core_analysis.base import FilePathParams


class CsvReaderParams(FilePathParams):
    delimiter: Optional[str] = Field(default=None, description="Delimiter used in the CSV file")
    strict_header: bool = Field(default=True, description="Whether to raise an error if the header is not found")


def csv_reader(params: CsvReaderParams) -> pd.DataFrame:
    return pd.read_csv(params.path, delimiter=params.delimiter, header=0 if params.strict_header else None)


class CsvWriterParams(FilePathParams):
    delimiter: str = Field(default=",", description="Delimiter used in the CSV file")
    encoding: Optional[Literal["utf-8"]] = Field(default=None, description="Encoding used in the CSV file")
    index: bool = Field(default=False, description="Whether to write the index to the CSV file")


def csv_writer(data: pd.DataFrame, params: CsvWriterParams) -> None:
    data.to_csv(params.path, sep=params.delimiter, index=params.index, encoding=params.encoding)
