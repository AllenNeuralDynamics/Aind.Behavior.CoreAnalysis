from dataclasses import dataclass
from typing import Optional

import pandas as pd

from .base import DataStream, FilePathBaseParam


@dataclass
class CsvParams(FilePathBaseParam):
    """Parameters for CSV file processing.

    Extends the base file path parameters with CSV-specific options.

    Attributes:
        delimiter: Custom delimiter character for CSV parsing. If None, the default comma delimiter is used.
        strict_header: If True, treats the first row as a header. Otherwise, no header is assumed.
        index: Column name to set as the DataFrame index. If None, default numeric indices are used.
    """

    delimiter: Optional[str] = None
    strict_header: bool = True
    index: Optional[str] = None


class Csv(DataStream[pd.DataFrame, CsvParams]):
    """CSV file data stream provider.

    A data stream implementation for reading CSV files into pandas DataFrames
    with configurable parameters for delimiter, header handling, and indexing.

    Args:
        DataStream: Base class for data stream providers.

    Examples:
        ```python
        from contraqctor.contract.csv import Csv, CsvParams

        # Create and load a CSV stream
        params = CsvParams(path="data/european_data.csv", delimiter=";")
        csv_stream = Csv("measurements", reader_params=params)
        csv_stream.load()

        # Access the DataFrame
        df = csv_stream.data
        filtered = df[df["temperature"] > 25]
        ```
    """

    @staticmethod
    def _reader(params: CsvParams) -> pd.DataFrame:
        """Read CSV file into a pandas DataFrame.

        Args:
            params: Parameters for CSV reading configuration.

        Returns:
            pd.DataFrame: DataFrame containing the parsed CSV data.
        """
        data = pd.read_csv(params.path, delimiter=params.delimiter, header=0 if params.strict_header else None)
        if params.index is not None:
            data.set_index(params.index, inplace=True)
        return data

    make_params = CsvParams
