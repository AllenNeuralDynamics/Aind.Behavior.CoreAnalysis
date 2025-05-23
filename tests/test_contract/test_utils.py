import pytest

from aind_behavior_core_analysis.contract.base import DataStreamCollection
from aind_behavior_core_analysis.contract.utils import load_branch

from .conftest import SimpleDataStream, SimpleParams


class TestLoadBranch:
    """Tests for the load_branch function."""

    def test_load_branch_success(self, text_file):
        """Test load_branch with successful loads."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))
        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=text_file))

        collection = DataStreamCollection(name="collection", data_streams=[stream1, stream2])

        result = load_branch(collection)

        assert result == []  # No exceptions
        assert stream1.has_data
        assert stream2.has_data

    def test_load_branch_with_exception(self, text_file, temp_dir):
        """Test load_branch with an exception."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))

        nonexistent_path = temp_dir / "nonexistent.txt"
        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=nonexistent_path))

        collection = DataStreamCollection(name="collection", data_streams=[stream1, stream2])

        result = load_branch(collection)

        assert len(result) == 1
        assert result[0][0] == stream2
        assert isinstance(result[0][1], FileNotFoundError)

        assert stream1.has_data
        assert not stream2.has_data

    def test_load_branch_strict(self, text_file, temp_dir):
        """Test load_branch with strict=True."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))

        nonexistent_path = temp_dir / "nonexistent.txt"
        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=nonexistent_path))

        collection = DataStreamCollection(name="collection", data_streams=[stream1, stream2])

        with pytest.raises(FileNotFoundError):
            load_branch(collection, strict=True)
