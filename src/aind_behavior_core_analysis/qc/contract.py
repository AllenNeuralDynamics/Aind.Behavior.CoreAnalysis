import typing as t

from ..contract.base import DataStream
from .base import Suite


class ContractTestSuite(Suite):
    """Materializes the output of DataStream.load_all() into a test suite."""

    def __init__(
        self, loading_errors: list[tuple[DataStream, Exception]], exclude: t.Optional[list[DataStream]] = None
    ):
        self.loading_errors = loading_errors
        self.exclude = exclude if exclude is not None else []

    def test_has_errors_on_load(self):
        errors = [(ds, err) for ds, err in self.loading_errors if ds not in self.exclude]
        if errors:
            str_errors = "\n".join([f"{ds.resolved_name}" for ds, _ in errors])
            return self.fail_test(
                None,
                f"The following DataStreams raised errors on load: {str_errors}",
                context={"errors": errors},
            )
        else:
            return self.pass_test(None, "All DataStreams loaded successfully")

    def test_has_excluded_as_warnings(self):
        warnings = [(ds, err) for ds, err in self.loading_errors if ds in self.exclude]
        if warnings:
            return self.warn_test(
                None,
                f"Found {len(warnings)} DataStreams that raised ignored errors on load.",
                context={"warnings": warnings},
            )
        else:
            return self.pass_test(None, "No excluded DataStreams raised errors on load")
