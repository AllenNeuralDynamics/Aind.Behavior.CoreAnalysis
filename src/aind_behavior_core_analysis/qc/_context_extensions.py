import typing as t

TExportable = t.TypeVar("TExportable", bound=t.Any)

ASSET_RESERVED_KEYWORD = "asset"


class ContextExportableObj(t.Generic[TExportable]):
    def __init__(self, obj: TExportable) -> None:
        self._obj = obj

    @property
    def asset(self) -> TExportable:
        return self._obj

    @classmethod
    def as_context(self, asset: TExportable) -> t.Dict[str, t.Self]:
        return {ASSET_RESERVED_KEYWORD: ContextExportableObj(asset)}

    @property
    def asset_type(self) -> str:
        return type(self._obj)
