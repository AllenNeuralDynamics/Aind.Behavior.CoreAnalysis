from dataclasses import dataclass
from typing import Optional

import pandas as pd

from . import FilePathBaseParam
from ._core import DataStream


@dataclass
class CsvReaderParams(FilePathBaseParam):
    delimiter: Optional[str] = None
    strict_header: bool = True
    index: Optional[str] = None


class CsvDataStream(DataStream[pd.DataFrame, CsvReaderParams]):
    @staticmethod
    def _reader(params: CsvReaderParams) -> pd.DataFrame:
        data = pd.read_csv(params.path, delimiter=params.delimiter, header=0 if params.strict_header else None)
        if params.index is not None:
            data.set_index(params.index, inplace=True)
        return data

    parameters = CsvReaderParams
