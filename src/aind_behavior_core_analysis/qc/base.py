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

# Define context variables with default values
_elevate_skippable = contextvars.ContextVar("elevate_skippable", default=False)
_elevate_warning = contextvars.ContextVar("elevate_warning", default=False)
_allow_null_as_pass_ctx = contextvars.ContextVar("allow_null_as_pass", default=False)


@contextmanager
def allow_null_as_pass(value: bool = True):
    """Context manager to control whether null results are allowed as pass."""
    token = _allow_null_as_pass_ctx.set(value)
    try:
        yield
    finally:
        _allow_null_as_pass_ctx.reset(token)


@contextmanager
def elevated_skips(value: bool = True):
    """Context manager to control whether tests can be skipped."""
    token = _elevate_skippable.set(value)
    try:
        yield
    finally:
        _elevate_skippable.reset(token)


@contextmanager
def elevated_warnings(value: bool = True):
    """Context manager to control whether warnings are allowed."""
    token = _elevate_warning.set(value)
    try:
        yield
    finally:
        _elevate_warning.reset(token)


class Status(Enum):
    PASSED = auto()
    FAILED = auto()
    ERROR = auto()
    SKIPPED = auto()
    WARNING = auto()

    def __str__(self) -> str:
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
    def __call__(self) -> "Result" | t.Generator["Result", None, None]: ...

    @property
    def __name__(self) -> str: ...


TResult = t.TypeVar("TResult", bound=t.Any)


@dataclasses.dataclass
class Result(t.Generic[TResult]):
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
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
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
    def get_tests(self) -> t.Generator[ITest, None, None]:
        """Find all methods starting with 'test'."""
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if name.startswith("test"):
                yield method

    @property
    def description(self) -> t.Optional[str]:
        return getattr(self, "__doc__", None)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def _get_caller_info(self):
        """Get information about the calling function."""
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
    def pass_test(self) -> Result: ...

    @t.overload
    def pass_test(self, result: t.Any) -> Result: ...

    @t.overload
    def pass_test(self, result: t.Any, message: str) -> Result: ...

    @t.overload
    def pass_test(self, result: t.Any, *, context: t.Any) -> Result: ...

    @t.overload
    def pass_test(self, result: t.Any, message: str, *, context: t.Any) -> Result: ...

    def pass_test(
        self, result: t.Any = None, message: t.Optional[str] = None, *, context: t.Optional[t.Any] = None
    ) -> Result:
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
    def warn_test(self) -> Result: ...

    @t.overload
    def warn_test(self, result: t.Any) -> Result: ...

    @t.overload
    def warn_test(self, result: t.Any, message: str) -> Result: ...

    @t.overload
    def warn_test(self, result: t.Any, *, context: t.Any) -> Result: ...

    @t.overload
    def warn_test(self, result: t.Any, message: str, *, context: t.Any) -> Result: ...

    def warn_test(
        self, result: t.Any = None, message: t.Optional[str] = None, *, context: t.Optional[t.Any] = None
    ) -> Result:
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
    def fail_test(self) -> Result: ...

    @t.overload
    def fail_test(self, result: t.Any) -> Result: ...

    @t.overload
    def fail_test(self, result: t.Any, message: str) -> Result: ...

    @t.overload
    def fail_test(self, result: t.Any, message: str, *, context: t.Any) -> Result: ...

    def fail_test(
        self, result: t.Optional[t.Any] = None, message: t.Optional[str] = None, *, context: t.Optional[t.Any] = None
    ) -> Result:
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
    def skip_test(self) -> Result: ...

    @t.overload
    def skip_test(self, message: str) -> Result: ...

    @t.overload
    def skip_test(self, message: str, *, context: t.Any) -> Result: ...

    def skip_test(self, message: t.Optional[str] = None, *, context: t.Optional[t.Any] = None) -> Result:
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
        """Run before each test method."""
        pass

    def teardown(self) -> None:
        """Run after each test method."""
        pass

    def _process_test_result(
        self, result: t.Optional[Result], test_method: ITest, test_name: str, description: t.Optional[str]
    ) -> Result:
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
        for test in self.get_tests():
            yield from self.run_test(test)


@dataclasses.dataclass
class ResultsStatistics:
    passed: int
    failed: int
    error: int
    skipped: int
    warnings: int

    @property
    def total(self) -> int:
        return self.passed + self.failed + self.error + self.skipped + self.warnings

    @property
    def pass_rate(self) -> float:
        total = self.total
        return (self.passed / total) if total > 0 else 0.0

    def get_status_summary(self) -> str:
        return f"P:{self[Status.PASSED]} F:{self[Status.FAILED]} E:{self[Status.ERROR]} S:{self[Status.SKIPPED]} W:{self[Status.WARNING]}"

    def __getitem__(self, item: Status) -> int:
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
        stats = {status: sum(1 for r in results if r.status == status) for status in Status}
        return cls(
            passed=stats[Status.PASSED],
            failed=stats[Status.FAILED],
            error=stats[Status.ERROR],
            skipped=stats[Status.SKIPPED],
            warnings=stats[Status.WARNING],
        )


class Runner:
    def __init__(self, suites: t.Optional[t.List[Suite]] = None):
        self.suites = suites if suites is not None else []
        self._results: t.Optional[t.List[Result]] = None

    def add_suite(self, suite: Suite) -> "Runner":
        self.suites.append(suite)
        return self

    def _render_status_bar(self, stats: ResultsStatistics, bar_width: int = 20) -> str:
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
    ) -> t.List[Result]:
        """Run all tests in all suites with a rich progress display and aligned columns."""

        suite_tests = [(suite, list(suite.get_tests())) for suite in self.suites]
        test_count = sum(len(tests) for _, tests in suite_tests)

        suite_name_width = (
            max(len(getattr(suite, "name", suite.__class__.__name__)) for suite, _ in suite_tests)
            if suite_tests
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
                "[bold green]Total Progress".ljust(suite_name_width + test_name_width + 5), total=test_count
            )

            all_results = []

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
                    progress.advance(suite_task)

                if tests:
                    stats = ResultsStatistics.from_results(suite_results)
                    status_bar = self._render_status_bar(stats, bar_width)

                    summary_line = (
                        f"[cyan]{suite_name:<{suite_name_width}} | {status_bar} | {stats.get_status_summary()}"
                    )
                    progress.update(suite_task, description=summary_line)

                all_results.extend(suite_results)

            if test_count > 0:
                total_stats = ResultsStatistics.from_results(all_results)
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

    @staticmethod
    def print_results(
        all_results: t.List[Result],
        include: set[Status] = set((Status.FAILED, Status.ERROR, Status.WARNING)),
        *,
        render_context: bool = True,
        render_description: bool = True,
        render_traceback: bool = True,
        render_message: bool = True,
    ):
        if all_results:
            included_tests = [r for r in all_results if r.status in include]
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

                for idx, test_result in enumerate(included_tests, 1):
                    color = STATUS_COLOR[test_result.status]
                    console.print(
                        f"[bold {color}]{idx}. {test_result.suite_name}.{test_result.test_name} ({test_result.status.value})[/bold {color}]"
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
