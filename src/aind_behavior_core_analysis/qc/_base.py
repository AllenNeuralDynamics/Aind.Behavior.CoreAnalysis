import abc
import dataclasses
import functools
import inspect
import traceback
import typing
from enum import Enum
import rich.progress

_SKIPPABLE = True


def set_skippable_ctx(value: bool):
    global _SKIPPABLE
    _SKIPPABLE = value


class TestStatus(Enum):
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


class ITest(typing.Protocol):
    """Protocol for test functions."""

    def __call__(self) -> "TestResult":
        """Run the test on the given datastream and return a TestResult."""
        pass


class FailTest(Exception):
    def __init__(self, result: typing.Optional[typing.Any] = None, message: typing.Optional[str] = None):
        self.result = result
        self.message = message
        super().__init__(message)


class SkipTest(Exception):
    def __init__(self, message: typing.Optional[str] = None):
        self.message = message
        super().__init__(message)

    def fail(self):
        return FailTest(result=None, message=self.message)


@dataclasses.dataclass
class TestResult:
    status: TestStatus
    result: typing.Any
    test_name: str
    suite_name: str
    _test_reference: typing.Optional[ITest] = dataclasses.field(default=None, repr=False)
    message: typing.Optional[str] = None
    context: typing.Optional[typing.Any] = dataclasses.field(default=None, repr=False)
    description: typing.Optional[str] = dataclasses.field(default=None, repr=False)
    exception: typing.Optional[Exception] = dataclasses.field(default=None, repr=False)
    traceback: typing.Optional[str] = dataclasses.field(default=None, repr=False)


def wrap_test(  # noqa: C901
    func=None,
    *,
    message: typing.Optional[str | typing.Callable[[typing.Any], str]] = None,
    description: typing.Optional[str] = None,
):
    """
    Decorator for test methods that handles exceptions and standardizes results.

    Args:
        func: The function to decorate (automatically provided when used without parentheses)

    Kwargs:
        message (str, optional): A message to include in the test result.
            Can include '{result}' which will be formatted with the test result.
        description (str, optional): A description of what the test does.
    """

    def decorator(test_func): # noqa: C901
        @functools.wraps(test_func)
        def wrapper(*args, **kwargs): # noqa: C901
            test_name = test_func.__name__
            # Use the provided message or the function's docstring as the description
            test_description = description or getattr(test_func, "__doc__", None)
            # Determine suite_name from the first argument if it's a class instance
            suite_name = args[0].__class__.__name__ if args and hasattr(args[0], "__class__") else "NoSuite"

            try:
                result = test_func(*args, **kwargs)

            except SkipTest as e:
                tb = traceback.format_exc()
                if _SKIPPABLE:
                    return TestResult(
                        status=TestStatus.SKIPPED,
                        result=None,
                        test_name=test_name,
                        suite_name=suite_name,
                        description=test_description,
                        message=e.message,
                        exception=e,
                        traceback=tb,
                        _test_reference=test_func,
                    )
                else:
                    return TestResult(
                        status=TestStatus.FAILED,
                        result=None,
                        test_name=test_name,
                        suite_name=suite_name,
                        description=test_description,
                        message=e.message or "Test cannot be skipped",
                        exception=e,
                        traceback=tb,
                        _test_reference=test_func,
                    )
            except FailTest as e:
                tb = traceback.format_exc()
                return TestResult(
                    status=TestStatus.FAILED,
                    result=e.result,
                    test_name=test_name,
                    suite_name=suite_name,
                    description=test_description,
                    message=e.message,
                    exception=e,
                    traceback=tb,
                    _test_reference=test_func,
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
                    traceback=tb,
                    _test_reference=test_func,
                )
            else:
                formatted_message = None
                if message:
                    if isinstance(message, str):
                        formatted_message = message.format(result=result)
                    elif callable(message):
                        formatted_message = message(result)

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

                return TestResult(
                    status=TestStatus.PASSED,
                    result=result,
                    test_name=test_name,
                    suite_name=suite_name,
                    description=test_description,
                    message=formatted_message,
                    _test_reference=test_func,
                )
        
        # Mark the wrapper function as a test
        setattr(wrapper, '_tag_for_test', True)
        return wrapper

    if func is not None:
        return decorator(func)

    return decorator


class TestSuite(abc.ABC):
    def __init__(self, name: str, *args, **kwargs):
        super().__init__()
    
    def get_tests(self) -> typing.Generator[typing.Callable, None, None]:
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if getattr(method, '_tag_for_test', None) is not None:
                yield method

    def run_all(self) -> typing.Generator[TestResult, None, None]:
        for test in self.get_tests():
            yield test()


class TestRunner:
    def __init__(self, suites: typing.Optional[typing.List[TestSuite]] = None):
        self.suites = suites if suites is not None else []
        self._results: typing.Optional[typing.List[TestResult]] = None

    def add_suite(self, suite: TestSuite) -> "TestRunner":
        self.suites.append(suite)
        return self

    def _calculate_statistics(self, results: typing.List[TestResult]) -> typing.Dict:
        total = len(results)
        stats = {status: sum(1 for r in results if r.status == status) for status in TestStatus}
        
        return {
            "total": total,
            "passed": stats[TestStatus.PASSED],
            "failed": stats[TestStatus.FAILED],
            "errors": stats[TestStatus.ERROR],
            "skipped": stats[TestStatus.SKIPPED],
            "pass_rate": (stats[TestStatus.PASSED] / total) if total > 0 else 0
        }

    def _render_status_bar(self, stats: typing.Dict, bar_width: int = 20) -> str:
        total = stats["total"]
        if total == 0:
            return ""
            
        status_bar = ""
        if stats["passed"]: 
            status_bar += f"[green]{'█' * int(bar_width * stats['passed']/total)}[/green]"
        if stats["failed"]:
            status_bar += f"[red]{'█' * int(bar_width * stats['failed']/total)}[/red]"
        if stats["errors"]:
            status_bar += f"[bright_red]{'█' * int(bar_width * stats['errors']/total)}[/bright_red]"
        if stats["skipped"]:
            status_bar += f"[yellow]{'█' * int(bar_width * stats['skipped']/total)}[/yellow]"
            
        return status_bar

    def _get_status_summary(self, stats: typing.Dict) -> str:
        return f"P:{stats['passed']} F:{stats['failed']} E:{stats['errors']} S:{stats['skipped']}"
    
    def run_all_with_progress(self) -> typing.List[TestResult]:
        """Run all tests in all suites with a rich progress display and aligned columns."""
        
        suite_tests = [(suite, list(suite.get_tests())) for suite in self.suites]
        test_count = sum(len(tests) for _, tests in suite_tests)
        
        suite_name_width = max(len(suite.__class__.__name__) for suite, _ in suite_tests) if suite_tests else 10
        test_name_width = 20 # To render the test name during progress
        bar_width = 20
        
        progress_format = [
            f"[progress.description]{{task.description:<{suite_name_width + test_name_width + 5}}}",
            rich.progress.BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            rich.progress.TimeRemainingColumn(),
        ]
        
        with rich.progress.Progress(*progress_format) as progress:
            total_task = progress.add_task("[bold green]Total Progress".ljust(suite_name_width + test_name_width + 5), total=test_count)
            
            all_results = []
            
            for suite, tests in suite_tests:
                suite_name = suite.__class__.__name__ # TODO replace wiht some "name" property
                suite_task = progress.add_task(f"[cyan]{suite_name}".ljust(suite_name_width + 5), total=len(tests))
                suite_results = []
                
                for test in tests:
                    test_name = test.__name__
                    test_desc = f"[cyan]{suite_name:<{suite_name_width}} • {test_name:<{test_name_width}}"
                    progress.update(suite_task, description=test_desc)
                    
                    result = test()
                    
                    suite_results.append(result)
                    
                    progress.advance(total_task)
                    progress.advance(suite_task)
                
                if tests:
                    stats = self._calculate_statistics(suite_results)
                    status_bar = self._render_status_bar(stats, bar_width)
                    summary = self._get_status_summary(stats)
                    
                    summary_line = f"[cyan]{suite_name:<{suite_name_width}} | {status_bar} | {summary}"
                    progress.update(suite_task, description=summary_line)
            
                all_results.extend(suite_results)
                
            if test_count > 0:
                total_stats = self._calculate_statistics(all_results)
                total_status_bar = self._render_status_bar(total_stats, bar_width+1)
                total_summary = self._get_status_summary(total_stats)
                
                total_line = f"[bold green]Total{' ':{suite_name_width-5}} | {total_status_bar} | {total_summary}"
                progress.update(total_task, description=total_line)
        
        self._results = all_results
        return all_results
