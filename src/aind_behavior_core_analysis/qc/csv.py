import pandas as pd

from ..contract.csv import Csv
from .base import Suite


class CsvTestSuite(Suite):
    """Test suite to check if CSV files generated are well formatted."""

    def __init__(self, data_stream: Csv):
        self.data_stream = data_stream

    def test_is_instance_of_pandas_dataframe(self):
        """
        Check if the data stream is a pandas DataFrame.
        """
        if not self.data_stream.has_data:
            return self.fail_test(None, "Data stream does not have loaded data")
        if not isinstance(self.data_stream.data, pd.DataFrame):
            return self.fail_test(None, "Data stream is not a pandas DataFrame")
        return self.pass_test(None, "Data stream is a pandas DataFrame")

    def test_is_not_empty(self):
        """
        Check if the DataFrame is not empty.
        """
        df = self.data_stream.data
        if df.empty:
            return self.fail_test(None, "Data stream is empty")
        return self.pass_test(None, "Data stream is not empty")

    def test_infer_missing_headers(self):
        """
        Infer if the DataFrame was loaded from a CSV without headers.
        """
        if not self.data_stream.reader_params.strict_header:
            return self.skip_test("CSV was loaded with strict_header=False")

        df = self.data_stream.data
        if df.empty or len(df.columns) == 0:
            return self.fail_test(None, "Data stream is empty or has no columns")

        # Check if column names are default integer indexes (0, 1, 2...)
        if all(isinstance(col, int) or (isinstance(col, str) and col.isdigit()) for col in df.columns):
            return self.fail_test(None, "Data stream has non-integer column names")

        return self.pass_test(None, "DataFramed was likely loaded from a CSV with headers")
