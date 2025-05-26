import pytest

from contraqctor.contract.text import Text, TextParams


class TestText:
    """Tests for the Text class."""

    def test_read_text(self, text_file):
        """Test reading a text file."""
        text_stream = Text(name="test", reader_params=TextParams(path=text_file))

        data = text_stream.read()
        assert data == "Test content"

    def test_load_text(self, text_file):
        """Test loading a text file."""
        text_stream = Text(name="test", reader_params=TextParams(path=text_file))

        text_stream.load()
        assert text_stream.has_data
        assert text_stream.data == "Test content"

    def test_custom_encoding(self, temp_dir):
        """Test reading a text file with custom encoding."""
        file_path = temp_dir / "utf16.txt"
        text = "Hello, 世界!"

        with open(file_path, "w", encoding="utf-16") as f:
            f.write(text)

        text_stream = Text(name="test", reader_params=TextParams(path=file_path, encoding="utf-16"))

        data = text_stream.read()
        assert data == text

    def test_nonexistent_file(self, temp_dir):
        """Test reading a nonexistent file."""
        nonexistent_path = temp_dir / "nonexistent.txt"

        text_stream = Text(name="test", reader_params=TextParams(path=nonexistent_path))

        with pytest.raises(FileNotFoundError):
            text_stream.read()

    def test_empty_file(self, temp_dir):
        """Test reading an empty file."""
        file_path = temp_dir / "empty.txt"
        with open(file_path, "w") as _:
            pass

        text_stream = Text(name="test", reader_params=TextParams(path=file_path))

        data = text_stream.read()
        assert data == ""
