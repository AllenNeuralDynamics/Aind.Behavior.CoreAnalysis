import pytest

from aind_behavior_core_analysis import _typing
from aind_behavior_core_analysis.contract._core import DataStreamCollection

from .conftest import SimpleDataStream, SimpleParams


class TestDataStream:
    """Tests for the DataStream class."""

    def test_creation(self, text_file):
        """Test creating a DataStream."""
        stream = SimpleDataStream(name="test", description="Test stream", reader_params=SimpleParams(path=text_file))

        assert stream.name == "test"
        assert stream.description == "Test stream"
        assert not stream.is_collection
        assert stream.parent is None
        assert not stream.has_data

        with pytest.raises(ValueError):
            # Accessing data before loading should raise ValueError
            _ = stream.data

    def test_load(self, text_file):
        """Test loading data from a DataStream."""
        stream = SimpleDataStream(name="test", reader_params=SimpleParams(path=text_file))

        stream.load()
        assert stream.has_data
        assert stream.data == "Test content"

    def test_read(self, text_file):
        """Test reading data without loading it."""
        stream = SimpleDataStream(name="test", reader_params=SimpleParams(path=text_file))

        data = stream.read()
        assert data == "Test content"
        assert not stream.has_data  # read() doesn't store the data

    def test_bind_reader_params(self, text_file):
        """Test post-instantiating binding of reader parameters."""
        stream = SimpleDataStream(name="test")

        assert _typing.is_unset(stream.reader_params)

        stream.bind_reader_params(SimpleParams(path=text_file))
        assert not _typing.is_unset(stream.reader_params)

        with pytest.raises(ValueError):
            # Binding params again should raise ValueError
            stream.bind_reader_params(SimpleParams(path=text_file))

    def test_at_not_implemented(self):
        """Test that at() method raises NotImplementedError."""
        stream = SimpleDataStream(name="test")

        with pytest.raises(NotImplementedError):
            stream.at("key")


class TestDataStreamCollection:
    """Tests for the DataStreamCollection annonymous class."""

    def test_creation(self, text_file):
        """Test creating a DataStreamCollection."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))
        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=text_file))

        collection = DataStreamCollection(
            name="collection", description="Test collection", data_streams=[stream1, stream2]
        )

        assert collection.name == "collection"
        assert collection.description == "Test collection"
        assert collection.is_collection
        assert collection.has_data  # data_streams are set directly
        assert len(collection.data) == 2

    def test_at_method(self, text_file):
        """Test accessing streams with at() method."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))
        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=text_file))

        collection = DataStreamCollection(name="collection", data_streams=[stream1, stream2])

        assert collection.at("stream1") == stream1
        assert collection.at("stream2") == stream2

        with pytest.raises(KeyError):
            collection.at("nonexistent")

    def test_indexing(self, text_file):
        """Test accessing streams with indexing."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))
        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=text_file))

        collection = DataStreamCollection(name="collection", data_streams=[stream1, stream2])

        assert collection["stream1"] == stream1
        assert collection["stream2"] == stream2

        with pytest.raises(KeyError):
            collection["nonexistent"]

    def test_add_stream(self, text_file):
        """Test adding a stream to a collection."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))

        collection = DataStreamCollection(name="collection", data_streams=[stream1])

        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=text_file))

        collection.add_stream(stream2)
        assert len(collection.data) == 2
        assert collection.at("stream2") == stream2

        # Adding a stream with an existing name should raise KeyError
        stream3 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))

        with pytest.raises(KeyError) as e:
            collection.add_stream(stream3)

    def test_remove_stream(self, text_file):
        """Test removing a stream from a collection."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))
        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=text_file))

        collection = DataStreamCollection(name="collection", data_streams=[stream1, stream2])

        collection.remove_stream("stream1")
        assert len(collection.data) == 1

        with pytest.raises(KeyError):
            collection.at("stream1")

        with pytest.raises(KeyError):
            collection.remove_stream("nonexistent")

    def test_parent_references(self, text_file):
        """Test that parent references are properly set."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))
        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=text_file))

        collection = DataStreamCollection(name="collection", data_streams=[stream1, stream2])

        assert stream1.parent == collection
        assert stream2.parent == collection

    def test_walk_data_streams(self, text_file):
        """Test walking through data streams."""
        stream1 = SimpleDataStream(name="stream1", reader_params=SimpleParams(path=text_file))
        stream2 = SimpleDataStream(name="stream2", reader_params=SimpleParams(path=text_file))

        inner_collection = DataStreamCollection(name="inner", data_streams=[stream2])

        outer_collection = DataStreamCollection(name="outer", data_streams=[stream1, inner_collection])

        streams = list(outer_collection.walk_data_streams())
        assert len(streams) == 3  # stream1, stream2, and inner_collection
        assert stream1 in streams
        assert stream2 in streams
        assert inner_collection in streams

    def test_duplicate_names(self, text_file):
        """Test that duplicate names raise an error."""
        stream1 = SimpleDataStream(name="duplicate", reader_params=SimpleParams(path=text_file))
        stream2 = SimpleDataStream(name="duplicate", reader_params=SimpleParams(path=text_file))

        with pytest.raises(ValueError):
            DataStreamCollection(name="collection", data_streams=[stream1, stream2])
