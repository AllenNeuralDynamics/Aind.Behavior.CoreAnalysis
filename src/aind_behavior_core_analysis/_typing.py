from typing import Any, Generic, Protocol, TypeVar, final

import pydantic

TData = TypeVar("TData", bound=Any)

TReaderParams = TypeVar("TReaderParams", bound=pydantic.BaseModel, contravariant=True)
TWriterParams = TypeVar("TWriterParams", bound=pydantic.BaseModel, contravariant=True)
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


@final
class __UnsetParams(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True)


UnsetParams: TReaderParams | TWriterParams = __UnsetParams()  # type: ignore
UnsetReader: __UnsetReader = __UnsetReader()
UnsetWriter: __UnsetWriter = __UnsetWriter()
UnsetData: Any = object()


@final
class UndefinedParams(pydantic.BaseModel):
    pass
