import dataclasses
from pathlib import Path
from typing import Any, Callable, Generic, List, Optional, Type, TypeVar

from . import DataStream, DataStreamCollectionBase, FilePathBaseParam

_TDataStreamPathAware = TypeVar("TDataStreamPathAware", bound=DataStream[Any, FilePathBaseParam])


@dataclasses.dataclass
class FileMuxReaderParams(FilePathBaseParam, Generic[_TDataStreamPathAware]):
    include_glob_pattern: List[str]
    inner_data_stream: Type[_TDataStreamPathAware]
    inner_param_factory: Callable[[str], FilePathBaseParam]
    as_collection: bool = False
    exclude_glob_pattern: List[str] = dataclasses.field(default_factory=list)
    inner_descriptions: dict[str, Optional[str]] = dataclasses.field(default_factory=dict)


class FromFileMux(DataStreamCollectionBase[_TDataStreamPathAware, FileMuxReaderParams]):
    make_params = FileMuxReaderParams

    @staticmethod
    def _reader(params: FileMuxReaderParams[_TDataStreamPathAware]) -> List[_TDataStreamPathAware]:
        _hits: List[Path] = []
        for pattern in params.include_glob_pattern:
            _hits.extend(list(Path(params.path).glob(pattern)))
        for pattern in params.exclude_glob_pattern:
            _hits = [f for f in _hits if not f.match(pattern)]
        _hits = list(set(_hits))

        if len(list(set([f.stem for f in _hits]))) != len(_hits):
            raise ValueError(f"Duplicate stems found in glob pattern: {params.include_glob_pattern}.")

        _out: List[_TDataStreamPathAware] = []
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
