import dataclasses
import functools
import inspect
import traceback
import typing
from enum import Enum

from .. import DataStream


class TestStatus(Enum):
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


class ITest(typing.Protocol):
    """Protocol for test functions."""

    def __call__(self, datastream: DataStream) -> "TestResult":
        """Run the test on the given datastream and return a TestResult."""
        pass


@dataclasses.dataclass
class TestResult:
    status: TestStatus
    result: typing.Any
    test_name: str
    suite_name: str
    _test_reference: typing.Optional[ITest] = dataclasses.field(default=None, repr=False)
    message: typing.Optional[str] = dataclasses.field(default=None, repr=False)
    description: typing.Optional[str] = dataclasses.field(default=None, repr=False)
    context: typing.Optional[typing.Any] = dataclasses.field(default=None, repr=False)
    exception: typing.Optional[Exception] = None
    traceback: typing.Optional[str] = None


def wrap_test(message=None, description=None):  # noqa: C901
    """
    Decorator for test methods that handles exceptions and standardizes results.

    Args:
        message (str, optional): A message to include in the test result.
            Can include '{result}' which will be formatted with the test result.
        description (str, optional): A description of what the test does.
    """

    def decorator(test_func):
        @functools.wraps(test_func)
        def wrapper(self, datastream=None):
            if datastream is None:
                datastream = self.data_stream

            test_name = test_func.__name__
            # Use the provided message or the function's docstring as the description
            test_description = description or getattr(test_func, "__doc__", None)
            suite_name = self.__class__.__name__

            try:
                result = test_func(self, datastream)
            except Exception as e:
                tb = traceback.format_exc()
                return TestResult(
                    status=TestStatus.ERROR,
                    result=None,
                    test_name=test_name,
                    suite_name=suite_name,
                    description=test_description,
                    message=f"Error during test execution: {str(e)}",
                    exception=e,
                    traceback=tb,
                    _test_reference=test_func,
                )
            else:
                formatted_message = None
                if message:
                    try:
                        formatted_message = message.format(result=result)
                    except (KeyError, ValueError, AttributeError):
                        formatted_message = message
    
                if isinstance(result, TestResult):
                    if not result.test_name:
                        result.test_name = test_name
                    if not result.suite_name:
                        result.suite_name = suite_name
                    if not result.description and test_description:
                        result.description = test_description
                    if not result.message and formatted_message:
                        result.message = formatted_message
                    if not result._test_reference:
                        result._test_reference = test_func
                    return result

                if isinstance(result, bool):
                    status = TestStatus.PASSED if result else TestStatus.FAILED
                else:
                    status = TestStatus.PASSED

                return TestResult(
                    status=status,
                    result=result,
                    test_name=test_name,
                    suite_name=suite_name,
                    description=test_description,
                    message=formatted_message,
                    _test_reference=test_func,
                )

        return wrapper

    # Handle case where decorator is used without arguments
    if callable(message):
        test_func = message
        message = None
        return decorator(test_func)

    return decorator


class TestSuite:
    def __init__(self, data_stream: DataStream):
        self.data_stream = data_stream

    def get_tests(self) -> typing.Generator[typing.Callable, None, None]:
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if name.startswith("test_"):
                yield method

    def run_all(self) -> typing.Generator[TestResult, None, None]:
        for test in self.get_tests():
            yield test(self.data_stream)


class TestRunner:
    def __init__(self):
        self.suites: list[TestSuite] = []

    def add_suite(self, suite: TestSuite) -> "TestRunner":
        self.suites.append(suite)
        return self

    def run_all(self) -> typing.List[TestResult]:
        results = []
        for suite in self.suites:
            results.extend(suite.run_all())
        return results

    def generate_report(self, results: typing.List[TestResult]) -> typing.Dict:
        total = len(results)
        passed = sum(1 for r in results if r.status == TestStatus.PASSED)
        failed = sum(1 for r in results if r.status == TestStatus.FAILED)
        errors = sum(1 for r in results if r.status == TestStatus.ERROR)
        skipped = sum(1 for r in results if r.status == TestStatus.SKIPPED)

        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "skipped": skipped,
            "pass_rate": (passed / total) if total > 0 else 0,
            "results": results,
        }
