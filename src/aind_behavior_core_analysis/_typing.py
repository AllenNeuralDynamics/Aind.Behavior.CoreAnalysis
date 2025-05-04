from typing import Any, Generic, Protocol, TypeAlias, TypeVar, Union, cast, final

TData = TypeVar("TData", bound=Union[Any, "_UnsetData"])

TReaderParams = TypeVar("TReaderParams", contravariant=True)
TWriterParams = TypeVar("TWriterParams", contravariant=True)
TData_co = TypeVar("TData_co", covariant=True)
TData_contra = TypeVar("TData_contra", contravariant=True)


class IReader(Protocol, Generic[TData_co, TReaderParams]):
    def __call__(self, params: TReaderParams) -> TData_co: ...


@final
class _UnsetReader(IReader[TData, TReaderParams]):
    def __call__(self, params: Any) -> Any:
        raise NotImplementedError("Reader is not set.")


@final
class _UnsetParams:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "<UnsetParams>"

    def __str__(self):
        return "<UnsetParams>"


@final
class _UnsetData:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "<UnsetData>"

    def __str__(self):
        return "<UnsetData>"


UnsetParams = cast(Any, _UnsetParams())
UnsetReader: _UnsetReader = _UnsetReader()
UnsetData: Any = _UnsetData()
UnsetParamsType: TypeAlias = _UnsetParams
