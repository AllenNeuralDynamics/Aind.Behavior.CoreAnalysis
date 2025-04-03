import dataclasses
import os
from pathlib import Path
from typing import Callable, Dict, Generic, List

from aind_behavior_core_analysis import DataStream, _typing


@dataclasses.dataclass
class MuxReaderParams(Generic[_typing.TData_co, _typing.TReaderParams]):
    path: os.PathLike
    glob_pattern: List[str]
    inner_reader: _typing.IReader[_typing.TData_co, _typing.TReaderParams]
    inner_reader_params_factory: Callable[[os.PathLike], _typing.TReaderParams]


def file_pattern_mux_reader(
    params: MuxReaderParams[_typing.TData, _typing.TReaderParams],
) -> Dict[str, DataStream[_typing.TData, _typing.TReaderParams, _typing.NullParams]]:
    def _inner_reader(
        path: os.PathLike = params.path,
    ) -> DataStream[_typing.TData, _typing.TReaderParams, _typing.NullParams]:
        return params.inner_reader(params.inner_reader_params_factory(path))

    _hits: List[Path] = []
    for pattern in params.glob_pattern:
        _hits.extend(list(Path(params.path).glob(pattern)))
    _hits = list(set(_hits))

    _out: Dict[str, DataStream[_typing.TData, _typing.TReaderParams, _typing.NullParams]] = {
        f.stem: _inner_reader(f) for f in _hits
    }
    return _out
