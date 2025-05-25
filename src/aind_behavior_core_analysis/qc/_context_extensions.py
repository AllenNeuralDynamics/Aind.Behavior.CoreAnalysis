import typing as t

TExportable = t.TypeVar("TExportable", bound=t.Any)

ASSET_RESERVED_KEYWORD = "asset"


class ContextExportableObj(t.Generic[TExportable]):
    """Container for exportable objects in test contexts.

    Provides a standardized way to include exportable objects (like figures or
    reports) in test result contexts, allowing them to be properly handled
    by reporting tools.

    Attributes:
        _obj: The exportable object being wrapped.
    """

    def __init__(self, obj: TExportable) -> None:
        """Initialize the context exportable object container.

        Args:
            obj: The object to wrap for export.
        """
        self._obj = obj

    @property
    def asset(self) -> TExportable:
        """Get the wrapped exportable object.

        Returns:
            TExportable: The wrapped object.
        """
        return self._obj

    @classmethod
    def as_context(self, asset: TExportable) -> t.Dict[str, "ContextExportableObj[TExportable]"]:
        """Create a standardized context dictionary for the exportable object.

        This method wraps the provided asset in a `ContextExportableObj` and
        includes it in a dictionary under a reserved keyword. This allows for
        consistent handling of exportable objects in test result contexts.

        Args:
            asset: The object to wrap and include in the context.

        Returns:
            Dict[str, ContextExportableObj]: A dictionary containing the wrapped
            asset under the reserved key.
        """
        return {ASSET_RESERVED_KEYWORD: ContextExportableObj(asset)}

    @property
    def asset_type(self) -> t.Type:
        """Get the type of the wrapped asset.

        Returns:
            Type: Type of the wrapped object.
        """
        return type(self._obj)
