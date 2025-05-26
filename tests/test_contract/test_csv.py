import pandas as pd

from contraqctor.contract.csv import Csv, CsvParams


class TestCsv:
    """Tests for the Csv class."""

    def test_read_csv(self, csv_file):
        """Test reading a CSV file."""
        csv_stream = Csv(name="test", reader_params=CsvParams(path=csv_file))

        data = csv_stream.read()
        assert isinstance(data, pd.DataFrame)
        assert list(data.columns) == ["name", "value", "timestamp"]
        assert list(data["name"]) == [f"Test {i}" for i in range(5)]
        assert list(data["value"]) == list(range(5))
        assert list(data["timestamp"]) == [i * 1000 for i in range(5)]

    def test_load_csv(self, csv_file):
        """Test loading a CSV file."""
        csv_stream = Csv(name="test", reader_params=CsvParams(path=csv_file))

        csv_stream.load()
        assert csv_stream.has_data
        assert isinstance(csv_stream.data, pd.DataFrame)
        assert list(csv_stream.data.columns) == ["name", "value", "timestamp"]

    def test_with_index(self, csv_file):
        """Test reading a CSV file with a specific index."""
        csv_stream = Csv(name="test", reader_params=CsvParams(path=csv_file, index="timestamp"))

        data = csv_stream.read()
        assert data.index.name == "timestamp"
        assert data.index.tolist() == [i * 1000 for i in range(5)]
        assert list(data.columns) == ["name", "value"]
        assert "timestamp" not in data.columns

    def test_empty_csv(self, temp_dir):
        """Test reading an empty CSV file (header only)."""
        file_path = temp_dir / "empty.csv"
        df = pd.DataFrame(columns=["name", "value", "timestamp"])
        df.to_csv(file_path, index=False)

        csv_stream = Csv(name="test", reader_params=CsvParams(path=file_path))

        data = csv_stream.read()
        assert isinstance(data, pd.DataFrame)
        assert list(data.columns) == ["name", "value", "timestamp"]
        assert len(data) == 0

    def test_no_header(self, temp_dir):
        """Test reading a CSV without strict header."""
        file_path = temp_dir / "no_header.csv"
        with open(file_path, "w") as f:
            f.write("a,b,c\n1,2,3\n4,5,6")

        csv_stream = Csv(name="test", reader_params=CsvParams(path=file_path, strict_header=False))

        data = csv_stream.read()
        assert isinstance(data, pd.DataFrame)
        # With strict_header=False, the first row becomes data, not headers
        assert data.iloc[0, 0] == "a"
        assert data.iloc[0, 1] == "b"
