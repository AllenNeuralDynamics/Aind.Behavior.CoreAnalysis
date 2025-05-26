import abc
import contextvars
import dataclasses
import functools
import inspect
import traceback
import typing as t
from contextlib import contextmanager
from enum import Enum, auto

import rich.progress
from rich.console import Console
from rich.syntax import Syntax

_elevate_skippable = contextvars.ContextVar("elevate_skippable", default=False)
_elevate_warning = contextvars.ContextVar("elevate_warning", default=False)
_allow_null_as_pass_ctx = contextvars.ContextVar("allow_null_as_pass", default=False)


@contextmanager
def allow_null_as_pass(value: bool = True):
    """Context manager to control whether null results are allowed as pass.

    When enabled, tests that return None will be treated as passing tests
    rather than producing errors.

    Args:
        value: True to allow null results as passing, False otherwise.
    """
    token = _allow_null_as_pass_ctx.set(value)
    try:
        yield
    finally:
        _allow_null_as_pass_ctx.reset(token)


@contextmanager
def elevated_skips(value: bool = True):
    """Context manager to control whether skipped tests are treated as failures.

    When enabled, skipped tests will be treated as failing tests rather than
    being merely marked as skipped.

    Args:
        value: True to elevate skipped tests to failures, False otherwise.
    """
    token = _elevate_skippable.set(value)
    try:
        yield
    finally:
        _elevate_skippable.reset(token)


@contextmanager
def elevated_warnings(value: bool = True):
    """Context manager to control whether warnings are treated as failures.

    When enabled, warning results will be treated as failing tests rather than
    just being marked as warnings.

    Args:
        value: True to elevate warnings to failures, False otherwise.
    """
    token = _elevate_warning.set(value)
    try:
        yield
    finally:
        _elevate_warning.reset(token)


class Status(Enum):
    """Enum representing possible test result statuses.

    Defines the different states a test can be in after execution.
    """

    PASSED = auto()
    FAILED = auto()
    ERROR = auto()
    SKIPPED = auto()
    WARNING = auto()

    def __str__(self) -> str:
        """Convert status to lowercase string representation.

        Returns:
            str: Lowercase string representation of the status.
        """
        return self.name.lower()


STATUS_COLOR = {
    Status.PASSED: "green",
    Status.FAILED: "red",
    Status.ERROR: "bright_red",
    Status.SKIPPED: "yellow",
    Status.WARNING: "dark_orange",
}


@t.runtime_checkable
class ITest(t.Protocol):
    """Protocol defining the interface for test functions.

    A test function should be callable, return a Result object or generator,
    and have a __name__ attribute.
    """

    def __call__(self) -> "Result" | t.Generator["Result", None, None]: ...

    @property
    def __name__(self) -> str: ...


TResult = t.TypeVar("TResult", bound=t.Any)


@dataclasses.dataclass
class Result(t.Generic[TResult]):
    """Container for test execution results.

    Stores the outcome of a test execution including status, returned value,
    contextual information, and any exception details.

    Attributes:
        status: The status of the test execution.
        result: The value returned by the test.
        test_name: Name of the test that generated this result.
        suite_name: Name of the test suite containing the test.
        _test_reference: Optional reference to the test function.
        message: Optional message describing the test outcome.
        context: Optional contextual data for the test result.
        description: Optional description of the test.
        exception: Optional exception that occurred during test execution.
        traceback: Optional traceback string if an exception occurred.
    """

    status: Status
    result: TResult
    test_name: str
    suite_name: str
    _test_reference: t.Optional[ITest] = dataclasses.field(default=None, repr=False)
    message: t.Optional[str] = None
    context: t.Optional[t.Any] = dataclasses.field(default=None, repr=False)
    description: t.Optional[str] = dataclasses.field(default=None, repr=False)
    exception: t.Optional[Exception] = dataclasses.field(default=None, repr=False)
    traceback: t.Optional[str] = dataclasses.field(default=None, repr=False)


def implicit_pass(func):
    """Decorator to automatically convert non-Result return values to passing results.

    If a test method returns something other than a Result object, this decorator
    will wrap it in a passing Result with the original return value.

    Args:
        func: The function to decorate.

    Returns:
        callable: Decorated function that ensures Result objects are returned.

    Raises:
        TypeError: If the decorated function is not a method of a Suite object.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        """Wrapper function to ensure the decorated function returns a Result."""
        result = func(self, *args, **kwargs)

        if isinstance(result, Result):
            return result

        # Just in case someone tries to do funny stuff
        if isinstance(self, Suite):
            return self.pass_test(result=result, message=f"Auto-converted return value: {result}")
        else:
            # Not in a Suite - can't convert properly
            raise TypeError(
                f"The auto_test decorator was used on '{func.__name__}' in a non-Suite "
                f"class ({self.__class__.__name__}). This is not supported."
            )

    return wrapper


class Suite(abc.ABC):
    """Base class for test suites.

    Provides the core functionality for defining, running, and reporting on tests.
    All test suites should inherit from this class and implement test methods
    that start with 'test'.
    """

    def get_tests(self) -> t.Generator[ITest, None, None]:
        """Find all methods starting with 'test'.

        Yields:
            ITest: Test methods found in the suite.
        """
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if name.startswith("test"):
                yield method

    @property
    def description(self) -> t.Optional[str]:
        """Get the description of the test suite from its docstring.

        Returns:
            Optional[str]: The docstring of the class, or None if not available.
        """
        return getattr(self, "__doc__", None)

    @property
    def name(self) -> str:
        """Get the name of the test suite.

        Returns:
            str: The name of the test suite class.
        """
        return self.__class__.__name__

    def _get_caller_info(self):
        """Get information about the calling function.

        Retrieves the name and docstring of the test method that called
        one of the result-generating methods.

        Returns:
            tuple: Containing the calling function name and its docstring.

        Raises:
            RuntimeError: If unable to retrieve the calling frame information.
        """
        if (f := inspect.currentframe()) is None:
            raise RuntimeError("Unable to retrieve the calling frame.")
        if (frame := f.f_back) is None:
            raise RuntimeError("Unable to retrieve the calling frame.")
        if (frame := frame.f_back) is None:  # Need to go one frame further as we're in a helper
            raise RuntimeError("Unable to retrieve the calling frame.")

        calling_func_name = frame.f_code.co_name
        description = getattr(frame.f_globals.get(calling_func_name), "__doc__", None)

        return calling_func_name, description

    @t.overload
    def pass_test(self) -> Result:
        """Create a passing test result with no result value.

        Returns:
            Result: A Result object with PASSED status and None result value.
        """
        ...

    @t.overload
    def pass_test(self, result: t.Any) -> Result:
        """Create a passing test result with a result value.

        Args:
            result: The value to include in the test result.

        Returns:
            Result: A Result object with PASSED status and the provided result value.
        """
        ...

    @t.overload
    def pass_test(self, result: t.Any, message: str) -> Result:
        """Create a passing test result with a result value and message.

        Args:
            result: The value to include in the test result.
            message: Message describing why the test passed.

        Returns:
            Result: A Result object with PASSED status, result value, and message.
        """
        ...

    @t.overload
    def pass_test(self, result: t.Any, *, context: t.Any) -> Result:
        """Create a passing test result with a result value and context data.

        Args:
            result: The value to include in the test result.
            context: Contextual data for the test result.

        Returns:
            Result: A Result object with PASSED status, result value, and context.
        """
        ...

    @t.overload
    def pass_test(self, result: t.Any, message: str, *, context: t.Any) -> Result:
        """Create a passing test result with a result value, message, and context data.

        Args:
            result: The value to include in the test result.
            message: Message describing why the test passed.
            context: Contextual data for the test result.

        Returns:
            Result: A Result object with PASSED status, result value, message, and context.
        """
        ...

    def pass_test(
        self, result: t.Any = None, message: t.Optional[str] = None, *, context: t.Optional[t.Any] = None
    ) -> Result:
        """Create a passing test result.

        Args:
            result: The value to include in the test result.
            message: Optional message describing why the test passed.
            context: Optional contextual data for the test result.

        Returns:
            Result: A Result object with PASSED status.
        """
        calling_func_name, description = self._get_caller_info()

        return Result(
            status=Status.PASSED,
            result=result,
            test_name=calling_func_name,
            suite_name=self.name,
            message=message,
            context=context,
            description=description,
        )

    @t.overload
    def warn_test(self) -> Result:
        """Create a warning test result with no result value.

        Returns:
            Result: A Result object with WARNING status (or FAILED if warnings are elevated)
                   and None result value.
        """
        ...

    @t.overload
    def warn_test(self, result: t.Any) -> Result:
        """Create a warning test result with a result value.

        Args:
            result: The value to include in the test result.

        Returns:
            Result: A Result object with WARNING status (or FAILED if warnings are elevated)
                   and the provided result value.
        """
        ...

    @t.overload
    def warn_test(self, result: t.Any, message: str) -> Result:
        """Create a warning test result with a result value and message.

        Args:
            result: The value to include in the test result.
            message: Message describing the warning.

        Returns:
            Result: A Result object with WARNING status (or FAILED if warnings are elevated),
                   result value, and message.
        """
        ...

    @t.overload
    def warn_test(self, result: t.Any, *, context: t.Any) -> Result:
        """Create a warning test result with a result value and context data.

        Args:
            result: The value to include in the test result.
            context: Contextual data for the test result.

        Returns:
            Result: A Result object with WARNING status (or FAILED if warnings are elevated),
                   result value, and context.
        """
        ...

    @t.overload
    def warn_test(self, result: t.Any, message: str, *, context: t.Any) -> Result:
        """Create a warning test result with a result value, message, and context data.

        Args:
            result: The value to include in the test result.
            message: Message describing the warning.
            context: Contextual data for the test result.

        Returns:
            Result: A Result object with WARNING status (or FAILED if warnings are elevated),
                   result value, message, and context.
        """
        ...

    def warn_test(
        self, result: t.Any = None, message: t.Optional[str] = None, *, context: t.Optional[t.Any] = None
    ) -> Result:
        """Create a warning test result.

        Creates a result with WARNING status, or FAILED if warnings are elevated.

        Args:
            result: The value to include in the test result.
            message: Optional message describing the warning.
            context: Optional contextual data for the test result.

        Returns:
            Result: A Result object with WARNING or FAILED status.
        """
        calling_func_name, description = self._get_caller_info()

        return Result(
            status=Status.WARNING if not _elevate_warning.get() else Status.FAILED,
            result=result,
            test_name=calling_func_name,
            suite_name=self.name,
            message=message,
            context=context,
            description=description,
        )

    @t.overload
    def fail_test(self) -> Result:
        """Create a failing test result with no result value.

        Returns:
            Result: A Result object with FAILED status and None result value.
        """
        ...

    @t.overload
    def fail_test(self, result: t.Any) -> Result:
        """Create a failing test result with a result value.

        Args:
            result: The value to include in the test result.

        Returns:
            Result: A Result object with FAILED status and the provided result value.
        """
        ...

    @t.overload
    def fail_test(self, result: t.Any, message: str) -> Result:
        """Create a failing test result with a result value and message.

        Args:
            result: The value to include in the test result.
            message: Message describing why the test failed.

        Returns:
            Result: A Result object with FAILED status, result value, and message.
        """
        ...

    @t.overload
    def fail_test(self, result: t.Any, message: str, *, context: t.Any) -> Result:
        """Create a failing test result with a result value, message, and context data.

        Args:
            result: The value to include in the test result.
            message: Message describing why the test failed.
            context: Contextual data for the test result.

        Returns:
            Result: A Result object with FAILED status, result value, message, and context.
        """
        ...

    def fail_test(
        self, result: t.Optional[t.Any] = None, message: t.Optional[str] = None, *, context: t.Optional[t.Any] = None
    ) -> Result:
        """Create a failing test result.

        Args:
            result: The value to include in the test result.
            message: Optional message describing why the test failed.
            context: Optional contextual data for the test result.

        Returns:
            Result: A Result object with FAILED status.
        """
        calling_func_name, description = self._get_caller_info()

        return Result(
            status=Status.FAILED,
            result=result,
            test_name=calling_func_name,
            suite_name=self.name,
            message=message,
            context=context,
            description=description,
        )

    @t.overload
    def skip_test(self) -> Result:
        """Create a skipped test result with no message.

        Returns:
            Result: A Result object with SKIPPED status (or FAILED if skips are elevated)
                   and None result value.
        """
        ...

    @t.overload
    def skip_test(self, message: str) -> Result:
        """Create a skipped test result with a message.

        Args:
            message: Message explaining why the test was skipped.

        Returns:
            Result: A Result object with SKIPPED status (or FAILED if skips are elevated)
                   and the provided message.
        """
        ...

    @t.overload
    def skip_test(self, message: str, *, context: t.Any) -> Result:
        """Create a skipped test result with a message and context data.

        Args:
            message: Message explaining why the test was skipped.
            context: Contextual data for the test result.

        Returns:
            Result: A Result object with SKIPPED status (or FAILED if skips are elevated),
                   message, and context.
        """
        ...

    def skip_test(self, message: t.Optional[str] = None, *, context: t.Optional[t.Any] = None) -> Result:
        """Create a skipped test result.

        Creates a result with SKIPPED status, or FAILED if skips are elevated.

        Args:
            message: Optional message explaining why the test was skipped.
            context: Optional contextual data for the test result.

        Returns:
            Result: A Result object with SKIPPED or FAILED status.
        """
        calling_func_name, description = self._get_caller_info()
        return Result(
            status=Status.SKIPPED if not _elevate_skippable.get() else Status.FAILED,
            result=None,
            test_name=calling_func_name,
            suite_name=self.name,
            message=message,
            context=context,
            description=description,
        )

    def setup(self) -> None:
        """Run before each test method.

        This method can be overridden by subclasses to implement
        setup logic that runs before each test.
        """
        pass

    def teardown(self) -> None:
        """Run after each test method.

        This method can be overridden by subclasses to implement
        teardown logic that runs after each test.
        """
        pass

    def _process_test_result(
        self, result: t.Optional[Result], test_method: ITest, test_name: str, description: t.Optional[str]
    ) -> Result:
        """Process and validate a test result.

        Ensures that the result is properly formatted and contains all necessary information.

        Args:
            result: The result returned by the test method.
            test_method: The test method that produced the result.
            test_name: The name of the test method.
            description: The description of the test method.

        Returns:
            Result: A properly formatted Result object.

        Raises:
            TypeError: If the test method returns something other than a Result object or generator.
        """
        if result is None and _allow_null_as_pass_ctx.get():
            result = self.pass_test(None, "Test passed with <null> result implicitly.")

        if isinstance(result, Result):
            result._test_reference = test_method
            result.test_name = test_name
            result.suite_name = self.name
            result.description = description
            return result

        error_msg = f"Test method '{test_name}' must return a TestResult instance or generator, but got {type(result).__name__}."
        return Result(
            status=Status.ERROR,
            result=result,
            test_name=test_name,
            suite_name=self.name,
            description=description,
            message=error_msg,
            exception=TypeError(error_msg),
            _test_reference=test_method,
        )

    def run_test(self, test_method: ITest) -> t.Generator[Result, None, None]:
        """Run a single test method and yield its results.

        Handles setup, test execution, result processing, and teardown.

        Args:
            test_method: The test method to run.

        Yields:
            Result: Result objects produced by the test method.
        """
        test_name = test_method.__name__
        suite_name = self.name
        test_description = getattr(test_method, "__doc__", None)

        try:
            self.setup()
            result = test_method()
            if inspect.isgenerator(result):
                for sub_result in result:
                    yield self._process_test_result(sub_result, test_method, test_name, test_description)
            else:
                yield self._process_test_result(result, test_method, test_name, test_description)
        except Exception as e:
            tb = traceback.format_exc()
            yield Result(
                status=Status.ERROR,
                result=None,
                test_name=test_name,
                suite_name=suite_name,
                description=test_description,
                message=f"Error during test execution: {str(e)}",
                exception=e,
                traceback=tb,
                _test_reference=test_method,
            )
        finally:
            self.teardown()

    def run_all(self) -> t.Generator[Result, None, None]:
        """Run all test methods in the suite.

        Finds all test methods and runs them in sequence.

        Yields:
            Result: Result objects produced by all test methods.
        """
        for test in self.get_tests():
            yield from self.run_test(test)


@dataclasses.dataclass
class ResultsStatistics:
    """Statistics about test results.

    Aggregates counts of test results by status and provides methods for
    calculating statistics like pass rate.

    Attributes:
        passed: Number of passed tests.
        failed: Number of failed tests.
        error: Number of tests that produced errors.
        skipped: Number of skipped tests.
        warnings: Number of tests with warnings.
    """

    passed: int
    failed: int
    error: int
    skipped: int
    warnings: int

    @property
    def total(self) -> int:
        """Get the total number of tests.

        Returns:
            int: Sum of all test result counts.
        """
        return self.passed + self.failed + self.error + self.skipped + self.warnings

    @property
    def pass_rate(self) -> float:
        """Calculate the pass rate.

        Returns:
            float: Ratio of passed tests to total tests, or 0 if no tests.
        """
        total = self.total
        return (self.passed / total) if total > 0 else 0.0

    def get_status_summary(self) -> str:
        """Generate a compact string summary of result counts.

        Returns:
            str: Summary string with counts for each status type.
        """
        return f"P:{self[Status.PASSED]} F:{self[Status.FAILED]} E:{self[Status.ERROR]} S:{self[Status.SKIPPED]} W:{self[Status.WARNING]}"

    def __getitem__(self, item: Status) -> int:
        """Get the count for a specific status.

        Args:
            item: The status to get the count for.

        Returns:
            int: Number of tests with the specified status.

        Raises:
            KeyError: If an invalid status is provided.
        """
        if item == Status.PASSED:
            return self.passed
        elif item == Status.FAILED:
            return self.failed
        elif item == Status.ERROR:
            return self.error
        elif item == Status.SKIPPED:
            return self.skipped
        elif item == Status.WARNING:
            return self.warnings
        else:
            raise KeyError(f"Invalid key: {item}. Valid keys are: {list(Status)}")

    @classmethod
    def from_results(cls, results: t.List[Result]) -> "ResultsStatistics":
        """Create statistics from a list of test results.

        Args:
            results: List of test results to analyze.

        Returns:
            ResultsStatistics: Statistics object summarizing the results.
        """
        stats = {status: sum(1 for r in results if r.status == status) for status in Status}
        return cls(
            passed=stats[Status.PASSED],
            failed=stats[Status.FAILED],
            error=stats[Status.ERROR],
            skipped=stats[Status.SKIPPED],
            warnings=stats[Status.WARNING],
        )


class Runner:
    """Test runner for executing suites and reporting results.

    Handles executing test suites, collecting results, and generating reports.

    Attributes:
        suites: Dictionary mapping group names to lists of test suites.
        _results: Optional dictionary of collected test results by group.
    """

    _DEFAULT_TEST_GROUP = "Ungrouped"

    def __init__(self):
        """Initialize the test runner."""
        self.suites: t.Dict[t.Optional[str], t.List[Suite]] = {}
        self._results: t.Optional[t.Dict[t.Optional[str], t.List[Result]]] = None

    @t.overload
    def add_suite(self, suite: Suite) -> "Runner":
        """Add a test suite to the runner without specifying a group.

        Args:
            suite: Test suite to add.

        Returns:
            Runner: Self for method chaining.
        """
        ...

    def add_suite(self, suite: Suite, group: t.Optional[str] = None) -> "Runner":
        """Add a test suite to the runner.

        Args:
            suite: Test suite to add.
            group: Optional group name for organizing suites. Defaults to None.

        Returns:
            Runner: Self for method chaining.
        """
        self._update_suites(suite, group)
        return self

    def _update_suites(self, suite: Suite, group: t.Optional[str] = None) -> None:
        if group in self.suites:
            self.suites[group].append(suite)
        else:
            self.suites[group] = [suite]

    def _collect_suites(self) -> t.Dict[t.Optional[str], t.List[t.Tuple[Suite, t.List[ITest]]]]:
        grouped_suites: t.Dict[t.Optional[str], t.List[t.Tuple[Suite, t.List[ITest]]]] = {}
        for group, suites in self.suites.items():
            grouped_suites[group] = [(suite, list(suite.get_tests())) for suite in suites]
        return grouped_suites

    def _render_status_bar(self, stats: ResultsStatistics, bar_width: int = 20) -> str:
        """Render a colored status bar representing test result proportions.

        Args:
            stats: Statistics to render.
            bar_width: Width of the status bar in characters.

        Returns:
            str: Rich-formatted string containing the status bar.
        """
        total = stats.total
        if total == 0:
            return ""

        status_bar = ""
        _t_int = 0
        for status in Status:
            if stats[status]:
                color = STATUS_COLOR[status]
                _bar_width = int(bar_width * (stats[status] / total))
                _t_int += _bar_width
                status_bar += f"[{color}]{'█' * _bar_width}[/{color}]"
        status_bar += f"[default]{'█' * (bar_width - _t_int)}[/default]"

        return status_bar

    def run_all_with_progress(
        self,
        *,
        render_context: bool = True,
        render_description: bool = True,
        render_traceback: bool = True,
        render_message: bool = True,
    ) -> t.Dict[t.Optional[str], t.List[Result]]:
        """Run all tests in all suites with a rich progress display.

        Executes all tests with a visual progress bar and detailed reporting
        of test outcomes.

        Args:
            render_context: Whether to render test context in result output.
            render_description: Whether to render test descriptions in result output.
            render_traceback: Whether to render tracebacks for errors in result output.
            render_message: Whether to render test result messages in result output.

        Returns:
            Dict[Optional[str], List[Result]]: Results grouped by test group name.
        """

        grouped_suites = self._collect_suites()
        total_tests = sum(sum(len(tests) for _, tests in group) for group in grouped_suites.values())

        flatten_suite_tests = [
            (suite, tests) for group_suites in grouped_suites.values() for suite, tests in group_suites
        ]

        suite_name_width = (
            max(len(getattr(suite, "name", suite.__class__.__name__)) for suite, _ in flatten_suite_tests)
            if flatten_suite_tests
            else 10
        )
        test_name_width = 20  # To render the test name during progress
        bar_width = 20

        progress_format = [
            f"[progress.description]{{task.description:<{suite_name_width + test_name_width + 5}}}",
            rich.progress.BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            rich.progress.TimeElapsedColumn(),
        ]

        with rich.progress.Progress(*progress_format) as progress:
            total_task = progress.add_task(
                "[bold green]Total Progress".ljust(suite_name_width + test_name_width + 5), total=total_tests
            )

            all_results: t.Dict[t.Optional[str], t.List[Result]] = {}

            for group, suite_tests in grouped_suites.items():
                all_results[group] = []
                group_task = progress.add_task(
                    "[bold blue]Group Progress".ljust(suite_name_width + test_name_width + 5),
                    total=sum(len(tests) for _, tests in suite_tests),
                )
                for suite, tests in suite_tests:
                    suite_name = getattr(suite, "name", suite.__class__.__name__)
                    suite_task = progress.add_task(f"[cyan]{suite_name}".ljust(suite_name_width + 5), total=len(tests))
                    suite_results = []

                    for test in tests:
                        test_name = test.__name__
                        test_desc = f"[cyan]{suite_name:<{suite_name_width}} • {test_name:<{test_name_width}}"
                        progress.update(suite_task, description=test_desc)

                        test_results = list(suite.run_test(test))
                        suite_results.extend(test_results)

                        progress.advance(total_task)
                        progress.advance(group_task)
                        progress.advance(suite_task)

                    if tests:
                        stats = ResultsStatistics.from_results(suite_results)
                        status_bar = self._render_status_bar(stats, bar_width)

                        summary_line = (
                            f"[cyan]{suite_name:<{suite_name_width}} | {status_bar} | {stats.get_status_summary()}"
                        )
                        progress.update(suite_task, description=summary_line)

                    all_results[group].extend(suite_results)

                if len(all_results[group]) > 0:
                    group_stats = ResultsStatistics.from_results(all_results[group])
                    group_status_bar = self._render_status_bar(group_stats, bar_width)
                    _title = f"{group}" if group else self._DEFAULT_TEST_GROUP
                    group_line = f"[bold blue]{_title}{' ':{suite_name_width - len(_title)}} | {group_status_bar} | {group_stats.get_status_summary()}"
                    progress.update(group_task, description=group_line)

            if total_tests > 0:
                total_stats = ResultsStatistics.from_results(
                    [result for results in all_results.values() for result in results]
                )
                total_status_bar = self._render_status_bar(total_stats, bar_width)

                _title = "Total"
                total_line = f"[bold green]{_title}{' ':{suite_name_width - len(_title)}} | {total_status_bar} | {total_stats.get_status_summary()}"
                progress.update(total_task, description=total_line)

        self._results = all_results
        if self._results:
            self.print_results(
                self._results,
                render_description=render_description,
                render_traceback=render_traceback,
                render_message=render_message,
                render_context=render_context,
            )

        return all_results

    def print_results(
        self,
        all_results: t.Dict[t.Optional[str], t.List[Result]],
        include: set[Status] = set((Status.FAILED, Status.ERROR, Status.WARNING)),
        *,
        render_context: bool = True,
        render_description: bool = True,
        render_traceback: bool = True,
        render_message: bool = True,
    ):
        """Print detailed test results to the console.

        Args:
            all_results: Dictionary of results by group to print.
            include: Set of status types to include in the output.
            render_context: Whether to render test context.
            render_description: Whether to render test descriptions.
            render_traceback: Whether to render tracebacks for errors.
            render_message: Whether to render test result messages.
        """
        if all_results:
            idx = 0
            for group, group_tests in all_results.items():
                group = group or self._DEFAULT_TEST_GROUP
                included_tests = [r for r in group_tests if r.status in include]
                if included_tests:
                    console = Console()
                    console.print()

                    if include:
                        console.print("Including ", end="")
                        for i, status in enumerate(include):
                            color = STATUS_COLOR[status]
                            console.print(f"[{color}]{status}[/{color}]", end="")
                            if i < len(include) - 1:
                                console.print(", ", end="")
                        console.print()

                    console.print()

                    for test_result in included_tests:
                        color = STATUS_COLOR[test_result.status]
                        console.print(
                            f"[bold {color}]{idx}. [{group}] {test_result.suite_name}.{test_result.test_name}[/bold {color}]"
                        )

                        console.print(f"[{color}]Result:[/{color}] {test_result.result}")

                        if render_message and test_result.message:
                            console.print(f"[{color}]Message:[/{color}] {test_result.message}")

                        if render_description and test_result.description:
                            console.print(f"[{color}]Description:[/{color}] {test_result.description}")

                        if render_traceback and test_result.traceback:
                            console.print(f"[{color}]Traceback:[/{color}]")
                            syntax = Syntax(test_result.traceback, "pytb", theme="ansi", line_numbers=False)
                            console.print(syntax)

                        if render_context and test_result.context:
                            console.print(f"[{color}]Context:[/{color}] {test_result.context}")

                        console.print("=" * 80)
                        idx += 1
