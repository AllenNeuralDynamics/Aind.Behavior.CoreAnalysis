from dataclasses import dataclass

from .base import DataStream, FilePathBaseParam


@dataclass
class TextParams(FilePathBaseParam):
    """Parameters for text file processing.

    Extends the base file path parameters with text encoding configuration.

    Attributes:
        encoding: The character encoding to use when reading the text file. Defaults to UTF-8.
    """

    encoding: str = "UTF-8"


class Text(DataStream[str, TextParams]):
    """Text file data stream provider.

    A data stream implementation for reading text files as a single string
    with configurable character encoding.

    Args:
        DataStream: Base class for data stream providers.

    Examples:
        ```python
        from contraqctor.contract.text import Text, TextParams

        # Create and load a text stream
        params = TextParams(path="README.md")
        readme_stream = Text("readme", reader_params=params).load()

        # Access the content
        content = readme_stream.data
        ```
    """

    @staticmethod
    def _reader(params: TextParams) -> str:
        """Read text file into a string.

        Args:
            params: Parameters for text file reading configuration.

        Returns:
            str: String containing the contents of the text file.
        """
        with open(params.path, "r", encoding=params.encoding) as file:
            return file.read()

    make_params = TextParams
