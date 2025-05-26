import pytest

from contraqctor.contract.base import DataStream
from contraqctor.qc.base import Status
from contraqctor.qc.contract import ContractTestSuite


class MockDataStream(DataStream):
    """Mock DataStream class for testing."""

    def __init__(self, name="test"):
        super().__init__(name=name)
        self._resolved_name = name

    @property
    def resolved_name(self):
        return self._resolved_name


@pytest.fixture
def loading_errors():
    ds1 = MockDataStream(name="stream1")
    ds2 = MockDataStream(name="stream2")
    ds3 = MockDataStream(name="stream3")

    err1 = ValueError("Error loading stream1")
    err2 = FileNotFoundError("File not found for stream2")
    err3 = RuntimeError("Error in stream3")

    return [(ds1, err1), (ds2, err2), (ds3, err3)]


@pytest.fixture
def excluded_streams(loading_errors):
    return [loading_errors[0][0]]


class TestContractTestSuite:
    """Tests for the ContractTestSuite class."""

    def test_init(self, loading_errors, excluded_streams):
        """Test initializing the ContractTestSuite."""
        suite = ContractTestSuite(loading_errors)
        assert suite.loading_errors == loading_errors
        assert suite.exclude == []

        suite = ContractTestSuite(loading_errors, exclude=excluded_streams)
        assert suite.loading_errors == loading_errors
        assert suite.exclude == excluded_streams

    def test_has_errors_on_load_with_errors(self, loading_errors):
        """Test test_has_errors_on_load method with errors."""
        suite = ContractTestSuite(loading_errors)
        result = suite.test_has_errors_on_load()

        assert result.status == Status.FAILED
        assert "raised errors on load" in result.message
        assert "errors" in result.context
        assert len(result.context["errors"]) == len(loading_errors)

    def test_has_errors_on_load_no_errors(self):
        """Test test_has_errors_on_load method with no errors."""
        suite = ContractTestSuite([])
        result = suite.test_has_errors_on_load()

        assert result.status == Status.PASSED
        assert "All DataStreams loaded successfully" in result.message

    def test_has_errors_on_load_with_excludes(self, loading_errors, excluded_streams):
        """Test test_has_errors_on_load method with excluded streams."""
        suite = ContractTestSuite(loading_errors, exclude=excluded_streams)
        result = suite.test_has_errors_on_load()

        assert result.status == Status.FAILED
        assert "errors" in result.context
        assert len(result.context["errors"]) == len(loading_errors) - len(excluded_streams)
        excluded_names = [ds.resolved_name for ds in excluded_streams]
        for ds, _ in result.context["errors"]:
            assert ds.resolved_name not in excluded_names

    def test_has_excluded_as_warnings_with_excludes(self, loading_errors, excluded_streams):
        """Test test_has_excluded_as_warnings method with excluded streams."""
        suite = ContractTestSuite(loading_errors, exclude=excluded_streams)
        result = suite.test_has_excluded_as_warnings()

        assert result.status == Status.WARNING
        assert "warnings" in result.context
        assert len(result.context["warnings"]) == len(excluded_streams)
        for ds, _ in result.context["warnings"]:
            assert ds in excluded_streams

    def test_has_excluded_as_warnings_no_excludes(self, loading_errors):
        """Test test_has_excluded_as_warnings method with no excluded streams."""
        suite = ContractTestSuite(loading_errors)
        result = suite.test_has_excluded_as_warnings()

        assert result.status == Status.PASSED
        assert "No excluded DataStreams raised errors" in result.message

    def test_has_excluded_as_warnings_empty_errors(self, excluded_streams):
        """Test test_has_excluded_as_warnings with empty errors but excluded streams."""
        suite = ContractTestSuite([], exclude=excluded_streams)
        result = suite.test_has_excluded_as_warnings()

        assert result.status == Status.PASSED
        assert "No excluded DataStreams raised errors" in result.message
