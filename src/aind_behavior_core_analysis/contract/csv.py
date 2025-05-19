from dataclasses import dataclass
from typing import Optional

import pandas as pd

from ._core import DataStream, FilePathBaseParam


@dataclass
class CsvParams(FilePathBaseParam):
    delimiter: Optional[str] = None
    strict_header: bool = True
    index: Optional[str] = None


class Csv(DataStream[pd.DataFrame, CsvParams]):
    @staticmethod
    def _reader(params: CsvParams) -> pd.DataFrame:
        data = pd.read_csv(params.path, delimiter=params.delimiter, header=0 if params.strict_header else None)
        if params.index is not None:
            data.set_index(params.index, inplace=True)
        return data

    make_params = CsvParams
