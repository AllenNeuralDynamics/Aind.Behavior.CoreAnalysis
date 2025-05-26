"""Tests for the QC CSV module."""

from unittest.mock import Mock

import pandas as pd
import pytest

from contraqctor.qc.base import Status
from contraqctor.qc.csv import CsvTestSuite


class MockCsvStream:
    """Mock Csv class for testing."""

    def __init__(self, data=None, has_data=True, strict_header=True):
        self._data = data
        self._has_data = has_data
        self.reader_params = Mock(strict_header=strict_header, path="mock/path.csv", delimiter=",", index=None)

    @property
    def has_data(self):
        return self._has_data

    @property
    def data(self):
        if not self._has_data:
            raise ValueError("CSV data stream has no data")
        return self._data


@pytest.fixture
def empty_df():
    """Fixture providing an empty DataFrame."""
    return pd.DataFrame()


@pytest.fixture
def valid_df():
    """Fixture providing a valid DataFrame with meaningful column names."""
    return pd.DataFrame(
        {"column1": [1, 2, 3], "column2": ["a", "b", "c"]}, index=pd.Index([0.1, 0.2, 0.3], name="Seconds")
    )


@pytest.fixture
def numeric_columns_df():
    """Fixture providing a DataFrame with numeric column names."""
    return pd.DataFrame({0: [1, 2, 3], 1: ["a", "b", "c"]}, index=pd.Index([0.1, 0.2, 0.3], name="Seconds"))


@pytest.fixture
def string_digits_columns_df():
    """Fixture providing a DataFrame with string digits as column names."""
    return pd.DataFrame({"0": [1, 2, 3], "1": ["a", "b", "c"]}, index=pd.Index([0.1, 0.2, 0.3], name="Seconds"))


class TestCsvTestSuite:
    """Tests for the CsvTestSuite class."""

    def test_init(self, valid_df):
        """Test initializing the CsvTestSuite."""
        csv_stream = MockCsvStream(data=valid_df)
        suite = CsvTestSuite(csv_stream)

        assert suite.data_stream == csv_stream

    def test_is_instance_of_pandas_dataframe(self, valid_df):
        """Test test_is_instance_of_pandas_dataframe method."""
        csv_stream = MockCsvStream(data=valid_df)
        suite = CsvTestSuite(csv_stream)

        result = suite.test_is_instance_of_pandas_dataframe()
        assert result.status == Status.PASSED
        assert "is a pandas DataFrame" in result.message

        csv_stream = MockCsvStream(data="not a dataframe")
        suite = CsvTestSuite(csv_stream)
        result = suite.test_is_instance_of_pandas_dataframe()
        assert result.status == Status.FAILED
        assert "not a pandas DataFrame" in result.message

        csv_stream = MockCsvStream(has_data=False)
        suite = CsvTestSuite(csv_stream)
        result = suite.test_is_instance_of_pandas_dataframe()
        assert result.status == Status.FAILED
        assert "does not have loaded data" in result.message

    def test_is_not_empty(self, valid_df, empty_df):
        """Test test_is_not_empty method."""
        csv_stream = MockCsvStream(data=valid_df)
        suite = CsvTestSuite(csv_stream)
        result = suite.test_is_not_empty()
        assert result.status == Status.PASSED
        assert "not empty" in result.message

        csv_stream = MockCsvStream(data=empty_df)
        suite = CsvTestSuite(csv_stream)
        result = suite.test_is_not_empty()
        assert result.status == Status.FAILED
        assert "empty" in result.message

    def test_infer_missing_headers(self, valid_df, empty_df, numeric_columns_df, string_digits_columns_df):
        """Test test_infer_missing_headers method."""
        csv_stream = MockCsvStream(data=valid_df)
        suite = CsvTestSuite(csv_stream)
        result = suite.test_infer_missing_headers()
        assert result.status == Status.PASSED

        csv_stream = MockCsvStream(data=valid_df, strict_header=False)
        suite = CsvTestSuite(csv_stream)
        result = suite.test_infer_missing_headers()
        assert result.status == Status.SKIPPED
        assert "strict_header=False" in result.message

        csv_stream = MockCsvStream(data=empty_df)
        suite = CsvTestSuite(csv_stream)
        result = suite.test_infer_missing_headers()
        assert result.status == Status.FAILED
        assert "empty" in result.message

        csv_stream = MockCsvStream(data=numeric_columns_df)
        suite = CsvTestSuite(csv_stream)
        result = suite.test_infer_missing_headers()
        assert result.status == Status.FAILED
        assert "non-integer column names" in result.message

        csv_stream = MockCsvStream(data=string_digits_columns_df)
        suite = CsvTestSuite(csv_stream)
        result = suite.test_infer_missing_headers()
        assert result.status == Status.FAILED
        assert "non-integer column names" in result.message
