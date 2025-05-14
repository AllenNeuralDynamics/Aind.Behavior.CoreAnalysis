import typing
import typing_extensions
import dataclasses
import abc
import inspect
import functools
import traceback
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
    



def wrap_test(test_func):
    @functools.wraps(test_func)
    def wrapper(self, datastream=None):
        if datastream is None:
            datastream = self.data_stream
            
        test_name = test_func.__name__
        test_description = getattr(test_func, '__doc__', None)
        suite_name = self.__class__.__name__
        
        try:
            result = test_func(self, datastream)
            
            if isinstance(result, TestResult):
                if not hasattr(result, 'test_name') or not result.test_name:
                    result.test_name = test_name
                if not hasattr(result, 'suite_name') or not result.suite_name:
                    result.suite_name = suite_name
                result.description = test_description
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
            )
            
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
                traceback=tb
            )
    
    return wrapper


class TestSuite:
    """Base class for test suites."""
    
    def __init__(self, data_stream: DataStream):
        self.data_stream = data_stream
    
    def get_tests(self) -> typing.Generator[typing.Callable, None, None]:
        """Get all test methods in this suite."""
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if name.startswith('test_'):
                yield method

    def run_all(self) -> typing.Generator[TestResult, None, None]:
        """Run all tests in this suite."""
        for test in self.get_tests():
                yield test(self.data_stream)


class TestRunner:
    """Runs tests from multiple test suites."""
    
    def __init__(self):
        self.suites = []
    
    def add_suite(self, suite: TestSuite) -> 'TestRunner':
        """Add a test suite to the runner."""
        self.suites.append(suite)
        return self
    
    def run_all(self) -> typing.List[TestResult]:
        """Run all tests in all suites."""
        results = []
        for suite in self.suites:
            results.extend(suite.run_all())
        return results
    
    def generate_report(self, results: typing.List[TestResult]) -> typing.Dict:
        """Generate a summary report of test results."""
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
            "results": results
        }