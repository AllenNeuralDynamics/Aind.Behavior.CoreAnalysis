import os
from dataclasses import dataclass
from typing import Literal, Optional

import pandas as pd


@dataclass
class CsvReaderParams:
    path: os.PathLike
    delimiter: Optional[str] = None
    strict_header: bool = True


def csv_reader(params: CsvReaderParams) -> pd.DataFrame:
    return pd.read_csv(params.path, delimiter=params.delimiter, header=0 if params.strict_header else None)


@dataclass
class CsvWriterParams:
    path: os.PathLike
    delimiter: str = ","
    encoding: Optional[Literal["utf-8"]] = "utf-8"
    index: bool = False


def csv_writer(data: pd.DataFrame, params: CsvWriterParams) -> None:
    data.to_csv(params.path, sep=params.delimiter, index=params.index, encoding=params.encoding)
