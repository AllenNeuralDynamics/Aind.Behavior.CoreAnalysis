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

    Examples:
        ```python
        import matplotlib.pyplot as plt
        from contraqctor.qc._context_extensions import ContextExportableObj
        from contraqctor.qc.base import Suite

        class VisualizationTestSuite(Suite):
            def test_create_plot(self):
                # Create a matplotlib figure
                fig, ax = plt.subplots()
                ax.plot([1, 2, 3], [4, 5, 6])
                ax.set_title("Test Plot")

                # Add the figure to the test context
                context = ContextExportableObj.as_context(fig)

                # Return test result with the figure in context
                return self.pass_test(True, "Plot created successfully", context=context)
        ```
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

        Examples:
            ```python
            import matplotlib.pyplot as plt
            from contraqctor.qc._context_extensions import ContextExportableObj

            # Create a visualization
            fig, ax = plt.subplots()
            ax.plot([1, 2, 3], [4, 5, 6])

            # Create a context dictionary with the figure
            context = ContextExportableObj.as_context(fig)

            # The context can now be passed to test result methods
            return self.pass_test(True, "Analysis succeeded", context=context)
            ```
        """
        return {ASSET_RESERVED_KEYWORD: ContextExportableObj(asset)}

    @property
    def asset_type(self) -> t.Type:
        """Get the type of the wrapped asset.

        Returns:
            Type: Type of the wrapped object.
        """
        return type(self._obj)
