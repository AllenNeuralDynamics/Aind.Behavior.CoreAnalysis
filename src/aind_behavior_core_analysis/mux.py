import dataclasses
from pathlib import Path
from typing import Dict, Generic, List, TypeVar

from aind_behavior_core_analysis import DataStream, _typing

from . import FilePathBaseParam

_TPathAwareReaderParams = TypeVar("_TPathAwareReaderParams", bound=FilePathBaseParam)


@dataclasses.dataclass
class MuxReaderParams(FilePathBaseParam, Generic[_typing.TData_co, _TPathAwareReaderParams]):
    glob_pattern: List[str]
    inner_reader: _typing.IReader[_typing.TData_co, _TPathAwareReaderParams]
    inner_reader_params: _TPathAwareReaderParams


def file_pattern_mux_reader(
    params: MuxReaderParams[_typing.TData, _TPathAwareReaderParams],
) -> Dict[str, DataStream[_typing.TData, _TPathAwareReaderParams, _typing.NullParams]]:
    _hits: List[Path] = []
    for pattern in params.glob_pattern:
        _hits.extend(list(Path(params.path).glob(pattern)))
    _hits = list(set(_hits))

    if len(list(set([f.stem for f in _hits]))) != len(_hits):
        raise ValueError(f"Duplicate stems found in glob pattern: {params.glob_pattern}.")

    _out: Dict[str, DataStream[_typing.TData, _TPathAwareReaderParams, _typing.NullParams]] = {}
    for f in _hits:
        new_params = dataclasses.replace(
            params.inner_reader_params,
            path=f,
        )
        _out[f.stem] = DataStream[_typing.TData, _TPathAwareReaderParams, _typing.NullParams](
            reader=params.inner_reader, reader_params=new_params
        )
    return _out
