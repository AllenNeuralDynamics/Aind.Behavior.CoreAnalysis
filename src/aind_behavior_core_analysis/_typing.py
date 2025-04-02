from typing import Any, Generic, Protocol, TypeVar, final, TypeAlias

TData = TypeVar("TData", bound=Any)

TReaderParams = TypeVar("TReaderParams", bound=Any, contravariant=True)
TWriterParams = TypeVar("TWriterParams", bound=Any, contravariant=True)
TData_co = TypeVar("TData_co", covariant=True, bound=Any)
TData_contra = TypeVar("TData_contra", contravariant=True, bound=Any)


class IReader(Protocol, Generic[TData_co, TReaderParams]):
    def __call__(self, params: TReaderParams) -> TData_co: ...


class IWriter(Protocol, Generic[TData_contra, TWriterParams]):
    def __call__(self, data: TData_contra, params: TWriterParams) -> Any: ...


@final
class __UnsetReader(IReader[TData, TReaderParams]):
    def __call__(self, params: Any) -> Any:
        raise NotImplementedError("Reader is not set.")


@final
class __UnsetWriter(IWriter[TData, TWriterParams]):
    def __call__(self, data: Any, params: Any) -> None:
        raise NotImplementedError("Writer is not set.")


class _NullParams:
    def __init__(self):
        raise NotImplementedError("This class is not meant to be instantiated.")


UnsetParams: TReaderParams | TWriterParams = object()  # type: ignore
UnsetReader: __UnsetReader = __UnsetReader()
UnsetWriter: __UnsetWriter = __UnsetWriter()
UnsetData: Any = object()
NullParams: TypeAlias = _NullParams
