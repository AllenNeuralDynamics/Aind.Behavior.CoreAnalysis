import dataclasses
import os
from pathlib import Path
from typing import Any, Callable, Generic, List, Optional, Type, TypeVar

from .. import _typing
from .base import DataStream, DataStreamCollectionBase

_TDataStream = TypeVar("_TDataStream", bound=DataStream[Any, _typing.TReaderParams])


@dataclasses.dataclass
class MapFromPathsParams(Generic[_TDataStream]):
    """Parameters for creating multiple data streams from file paths.

    Defines parameters for locating files and creating data streams for each one.

    Attributes:
        paths: List of directory paths to search for files.
        include_glob_pattern: List of glob patterns to match files to include.
        inner_data_stream: Type of DataStream to create for each matched file.
        inner_param_factory: Function that creates reader params from file paths.
        as_collection: Whether to return results as a collection. Defaults to True.
        exclude_glob_pattern: List of glob patterns for files to exclude.
        inner_descriptions: Dictionary mapping file stems to descriptions for streams.

    """

    paths: List[os.PathLike]
    include_glob_pattern: List[str]
    inner_data_stream: Type[_TDataStream]
    inner_param_factory: Callable[[str], _typing.TReaderParams]
    as_collection: bool = True
    exclude_glob_pattern: List[str] = dataclasses.field(default_factory=list)
    inner_descriptions: dict[str, Optional[str]] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        """Validate and normalize the paths parameter after initialization.

        Ensures paths is a list and validates that at least one path is provided.

        Raises:
            ValueError: If no paths are provided.
        """
        if isinstance(self.paths, (str, os.PathLike)):
            self.paths = [self.paths]
        if len(self.paths) == 0:
            raise ValueError("At least one path must be provided.")


class MapFromPaths(DataStreamCollectionBase[_TDataStream, MapFromPathsParams]):
    """File path mapper data stream provider.

    A data stream implementation for creating multiple child data streams
    by searching for files matching glob patterns and creating a stream for each.

    Args:
        DataStreamCollectionBase: Base class for data stream collection providers.

    Examples:
        ```python
        from contraqctor.contract import mux, text

        # Define a factory function for TextParams
        def create_text_params(file_path):
            return text.TextParams(path=file_path)

        # Create and load a text file collection
        params = mux.MapFromPathsParams(
            paths=["documents/"],
            include_glob_pattern=["*.txt"],
            inner_data_stream=text.Text,
            inner_param_factory=create_text_params
        )

        docs = mux.MapFromPaths("documents", reader_params=params).load()
        readme = docs["readme"].data
        ```
    """

    make_params = MapFromPathsParams

    @staticmethod
    def _reader(params: MapFromPathsParams[_TDataStream]) -> List[_TDataStream]:
        """Create data streams for files matching the specified patterns.

        Args:
            params: Parameters for file path mapping configuration.

        Returns:
            List[_TDataStream]: List of data stream objects, one per matched file.

        Raises:
            ValueError: If duplicate file stems (names without extensions) are found.

        Examples:
            ```python
            from contraqctor.contract import mux, csv

            def make_csv_params(file_path):
                return csv.CsvParams(path=file_path)

            params = mux.MapFromPathsParams(
                paths=["data/sensors/"],
                include_glob_pattern=["*.csv"],
                inner_data_stream=csv.Csv,
                inner_param_factory=make_csv_params
            )

            # Get streams directly
            streams = mux.MapFromPaths._reader(params)
            ```
        """
        _hits: List[Path] = []

        for p in params.paths:
            for pattern in params.include_glob_pattern:
                _hits.extend(list(Path(p).glob(pattern)))
            for pattern in params.exclude_glob_pattern:
                _hits = [f for f in _hits if not f.match(pattern)]
            _hits = list(set(_hits))

        if len(list(set([f.stem for f in _hits]))) != len(_hits):
            raise ValueError(f"Duplicate stems found in glob pattern: {params.include_glob_pattern}.")

        _out: List[_TDataStream] = []
        _descriptions = params.inner_descriptions
        for f in _hits:
            _out.append(
                params.inner_data_stream(
                    name=f.stem,
                    description=_descriptions.get(f.stem, None),
                    reader_params=params.inner_param_factory(f),
                )
            )
        return _out
