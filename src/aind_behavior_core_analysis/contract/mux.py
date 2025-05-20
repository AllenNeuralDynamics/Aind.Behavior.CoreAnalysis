import dataclasses
import os
from pathlib import Path
from typing import Any, Callable, Generic, List, Optional, Type, TypeVar

from .. import _typing
from .base import DataStream, DataStreamCollectionBase

_TDataStream = TypeVar("_TDataStream", bound=DataStream[Any, _typing.TReaderParams])


@dataclasses.dataclass
class MapFromPathsParams(Generic[_TDataStream]):
    paths: List[os.PathLike]
    include_glob_pattern: List[str]
    inner_data_stream: Type[_TDataStream]
    inner_param_factory: Callable[[str], _typing.TReaderParams]
    as_collection: bool = True
    exclude_glob_pattern: List[str] = dataclasses.field(default_factory=list)
    inner_descriptions: dict[str, Optional[str]] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.paths, (str, os.PathLike)):
            self.paths = [self.paths]
        if len(self.paths) == 0:
            raise ValueError("At least one path must be provided.")


class MapFromPaths(DataStreamCollectionBase[_TDataStream, MapFromPathsParams]):
    make_params = MapFromPathsParams

    @staticmethod
    def _reader(params: MapFromPathsParams[_TDataStream]) -> List[_TDataStream]:
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
