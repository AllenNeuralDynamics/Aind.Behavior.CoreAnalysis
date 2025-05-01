import dataclasses
from pathlib import Path
from typing import Generic, List, TypeVar

from aind_behavior_core_analysis import DataStream, DataStreamGroup, _typing

from . import FilePathBaseParam

_TPathAwareReaderParams = TypeVar("_TPathAwareReaderParams", bound=FilePathBaseParam)

TDataStream = TypeVar("TDataStream", bound=DataStream)


@dataclasses.dataclass
class MuxReaderParams(FilePathBaseParam, Generic[_typing.TData_co, _TPathAwareReaderParams]):
    include_glob_pattern: List[str]
    inner_reader: _typing.IReader[_typing.TData_co, _TPathAwareReaderParams]
    inner_reader_params: _TPathAwareReaderParams
    as_data_stream_group: bool = False
    exclude_glob_pattern: List[str] = dataclasses.field(default_factory=list)


def file_pattern_mux_reader(
    params: MuxReaderParams[_typing.TData, _TPathAwareReaderParams],
) -> List[DataStream[_typing.TData, _TPathAwareReaderParams, _typing.UnsetParamsType]]:
    _hits: List[Path] = []
    for pattern in params.include_glob_pattern:
        _hits.extend(list(Path(params.path).glob(pattern)))
    for pattern in params.exclude_glob_pattern:
        _hits = [f for f in _hits if not f.match(pattern)]
    _hits = list(set(_hits))

    if len(list(set([f.stem for f in _hits]))) != len(_hits):
        raise ValueError(f"Duplicate stems found in glob pattern: {params.include_glob_pattern}.")

    _out: List[DataStream[_typing.TData, _TPathAwareReaderParams, _typing.UnsetParamsType]] = []
    for f in _hits:
        new_params = dataclasses.replace(
            params.inner_reader_params,
            path=f,
        )
        _constructor = DataStreamGroup if params.as_data_stream_group else DataStream
        # If we want to be extra safe, we should probably have a runtime check here to ensure the return
        # type of the reader is bound to the type of the DataStreamGroup. I will just ignore it for now.
        # Alternatively, we could split these into two functions, and narrow the type of the reader.
        _out.append(_constructor(name=f.stem, reader=params.inner_reader, reader_params=new_params))  # type: ignore
    return _out
