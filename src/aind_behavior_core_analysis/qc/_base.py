import abc
import dataclasses
import functools
import inspect
import traceback
import typing
from enum import Enum
from typing import Any, Generator, List, Optional, Protocol, TypeVar

import rich.progress
from rich.console import Console
from rich.syntax import Syntax

_SKIPPABLE = True
_ALLOW_NULL_AS_PASS = False


def set_allow_null_as_pass(value: bool):
    global _ALLOW_NULL_AS_PASS
    _ALLOW_NULL_AS_PASS = value


def set_skippable_ctx(value: bool):
    global _SKIPPABLE
    _SKIPPABLE = value


class TestStatus(Enum):
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"


STATUS_COLOR = {
    TestStatus.PASSED: "green",
    TestStatus.FAILED: "red",
    TestStatus.ERROR: "bright_red",
    TestStatus.SKIPPED: "yellow",
}


@typing.runtime_checkable
class ITest(Protocol):
    def __call__(self) -> "TestResult": ...

    @property
    def __name__(self) -> str: ...


TResult = TypeVar("TResult", bound=Any)


@dataclasses.dataclass
class TestResult(typing.Generic[TResult]):
    status: TestStatus
    result: TResult
    test_name: str
    suite_name: str
    _test_reference: Optional[ITest] = dataclasses.field(default=None, repr=False)
    message: Optional[str] = None
    context: Optional[Any] = dataclasses.field(default=None, repr=False)
    description: Optional[str] = dataclasses.field(default=None, repr=False)
    exception: Optional[Exception] = dataclasses.field(default=None, repr=False)
    traceback: Optional[str] = dataclasses.field(default=None, repr=False)


def implicit_pass(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)

        if isinstance(result, TestResult):
            return result

        # Just in case someone tries to do funny stuff
        if isinstance(self, TestSuite):
            return self.pass_test(result=result, message=f"Auto-converted return value: {result}")
        else:
            # Not in a TestSuite - can't convert properly
            raise TypeError(
                f"The auto_test decorator was used on '{func.__name__}' in a non-TestSuite "
                f"class ({self.__class__.__name__}). This is not supported."
            )

    return wrapper


class TestSuite(abc.ABC):
    def get_tests(self) -> Generator[ITest, None, None]:
        """Find all methods starting with 'test'."""
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if name.startswith("test"):
                yield method

    @typing.overload
    def pass_test(self) -> TestResult: ...

    @typing.overload
    def pass_test(self, result: Any) -> TestResult: ...

    @typing.overload
    def pass_test(self, result: Any, message: str) -> TestResult: ...

    @typing.overload
    def pass_test(self, result: Any, *, context: Any) -> TestResult: ...

    @typing.overload
    def pass_test(self, result: Any, message: str, *, context: Any) -> TestResult: ...

    def pass_test(
        self, result: Any = None, message: Optional[str] = None, *, context: Optional[Any] = None
    ) -> TestResult:
        if (f := inspect.currentframe()) is None:
            raise RuntimeError("Unable to retrieve the calling frame.")
        if (frame := f.f_back) is None:
            raise RuntimeError("Unable to retrieve the calling frame.")
        calling_func_name = frame.f_code.co_name

        return TestResult(
            status=TestStatus.PASSED,
            result=result,
            test_name=calling_func_name,
            suite_name=self.__class__.__name__,
            message=message,
            context=context,
            description=getattr(frame.f_globals.get(calling_func_name), "__doc__", None),
        )

    # Fail Test Method with Overloads
    @typing.overload
    def fail_test(self) -> TestResult: ...

    @typing.overload
    def fail_test(self, result: Any) -> TestResult: ...

    @typing.overload
    def fail_test(self, result: Any, message: str) -> TestResult: ...

    @typing.overload
    def fail_test(self, result: Any, message: str, *, context: Any) -> TestResult: ...

    def fail_test(
        self, result: Optional[Any] = None, message: Optional[str] = None, *, context: Optional[Any] = None
    ) -> TestResult:
        if (f := inspect.currentframe()) is None:
            raise RuntimeError("Unable to retrieve the calling frame.")
        if (frame := f.f_back) is None:
            raise RuntimeError("Unable to retrieve the calling frame.")
        calling_func_name = frame.f_code.co_name

        return TestResult(
            status=TestStatus.FAILED,
            result=result,
            test_name=calling_func_name,
            suite_name=self.__class__.__name__,
            message=message,
            context=context,
            description=getattr(frame.f_globals.get(calling_func_name), "__doc__", None),
        )

    # Skip Test Method with Overloads
    @typing.overload
    def skip_test(self) -> TestResult: ...

    @typing.overload
    def skip_test(self, message: str) -> TestResult: ...

    @typing.overload
    def skip_test(self, message: str, *, context: Any) -> TestResult: ...

    def skip_test(self, message: Optional[str] = None, *, context: Optional[Any] = None) -> TestResult:
        if (f := inspect.currentframe()) is None:
            raise RuntimeError("Unable to retrieve the calling frame.")
        if (frame := f.f_back) is None:
            raise RuntimeError("Unable to retrieve the calling frame.")
        calling_func_name = frame.f_code.co_name
        return TestResult(
            status=TestStatus.SKIPPED if _SKIPPABLE else TestStatus.FAILED,
            result=None,
            test_name=calling_func_name,
            suite_name=self.__class__.__name__,
            message=message,
            context=context,
            description=getattr(frame.f_globals.get(calling_func_name), "__doc__", None),
        )

    def run_test(self, test_method: ITest) -> TestResult:
        test_name = test_method.__name__
        suite_name = self.__class__.__name__
        description = getattr(test_method, "__doc__", None)

        try:
            result = test_method()
            if result is None and _ALLOW_NULL_AS_PASS:
                return self.pass_test(None, "Test passed with <null> result implicitly.")
            if not isinstance(result, TestResult):
                raise TypeError(
                    f"Test method '{test_name}' must return a TestResult instance, but got {type(result).__name__}."
                )
            return result

        except Exception as e:
            tb = traceback.format_exc()
            return TestResult(
                status=TestStatus.ERROR,
                result=None,
                test_name=test_name,
                suite_name=suite_name,
                description=description,
                message=f"Error during test execution: {str(e)}",
                exception=e,
                traceback=tb,
                _test_reference=test_method,
            )

    def run_all(self) -> Generator[TestResult, None, None]:
        for test in self.get_tests():
            yield self.run_test(test)


@dataclasses.dataclass
class TestStatistics:
    passed: int
    failed: int
    error: int
    skipped: int

    @property
    def total(self) -> int:
        return self.passed + self.failed + self.error + self.skipped

    @property
    def pass_rate(self) -> float:
        total = self.total
        return (self.passed / total) if total > 0 else 0.0

    def get_status_summary(self) -> str:
        return f"P:{self[TestStatus.PASSED]} F:{self[TestStatus.FAILED]} E:{self[TestStatus.ERROR]} S:{self[TestStatus.SKIPPED]}"

    def __getitem__(self, item: TestStatus) -> int:
        if item == TestStatus.PASSED:
            return self.passed
        elif item == TestStatus.FAILED:
            return self.failed
        elif item == TestStatus.ERROR:
            return self.error
        elif item == TestStatus.SKIPPED:
            return self.skipped
        else:
            raise KeyError(f"Invalid key: {item}. Valid keys are: {list(TestStatus)}")

    @classmethod
    def from_results(cls, results: List[TestResult]) -> "TestStatistics":
        stats = {status: sum(1 for r in results if r.status == status) for status in TestStatus}
        return cls(
            passed=stats[TestStatus.PASSED],
            failed=stats[TestStatus.FAILED],
            error=stats[TestStatus.ERROR],
            skipped=stats[TestStatus.SKIPPED],
        )


class TestRunner:
    def __init__(self, suites: Optional[List[TestSuite]] = None):
        self.suites = suites if suites is not None else []
        self._results: Optional[List[TestResult]] = None

    def add_suite(self, suite: TestSuite) -> "TestRunner":
        self.suites.append(suite)
        return self

    def _render_status_bar(self, stats: TestStatistics, bar_width: int = 20) -> str:
        total = stats.total
        if total == 0:
            return ""

        status_bar = ""
        _t = 0.0
        _t_int = 0
        for status in TestStatus:
            if stats[status]:
                color = STATUS_COLOR[status]
                _bar_width = int(bar_width * (stats[status] / total))
                _t_int += _bar_width
                status_bar += f"[{color}]{'█' * _bar_width}[/{color}]"
        status_bar += f"[default]{'█' * (bar_width - _t_int)}[/default]"

        return status_bar

    def run_all_with_progress(self) -> List[TestResult]:
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

                    result = suite.run_test(test)

                    suite_results.append(result)

                    progress.advance(total_task)
                    progress.advance(suite_task)

                if tests:
                    stats = TestStatistics.from_results(suite_results)
                    status_bar = self._render_status_bar(stats, bar_width)

                    summary_line = (
                        f"[cyan]{suite_name:<{suite_name_width}} | {status_bar} | {stats.get_status_summary()}"
                    )
                    progress.update(suite_task, description=summary_line)

                all_results.extend(suite_results)

            if test_count > 0:
                total_stats = TestStatistics.from_results(all_results)
                total_status_bar = self._render_status_bar(total_stats, bar_width)

                _title = "Total"
                total_line = f"[bold green]{_title}{' ':{suite_name_width - len(_title)}} | {total_status_bar} | {stats.get_status_summary()}"
                progress.update(total_task, description=total_line)

        self._results = all_results
        if self._results:
            self.print_results(self._results)

        return all_results

    @staticmethod
    def print_results(
        all_results: List[TestResult], include: set[TestStatus] = set((TestStatus.FAILED, TestStatus.ERROR))
    ):
        if all_results:
            included_tests = [r for r in all_results if r.status in include]
            if included_tests:
                console = Console()
                console.print()
                _include = "Including " + ", ".join([str(i.value) for i in include]) if include else ""
                console.print(f"[bold red]{_include}[/bold red]")
                console.print()

                for idx, failure in enumerate(included_tests, 1):
                    color = STATUS_COLOR[failure.status]
                    console.print(
                        f"[bold {color}]{idx}. {failure.suite_name}.{failure.test_name} ({failure.status.value})[/bold {color}]"
                    )

                    if failure.message:
                        console.print(f"[{color}]Message:[/{color}] {failure.message}")

                    if failure.description:
                        console.print(f"[{color}]Description:[/{color}] {failure.description}")

                    if failure.traceback:
                        console.print(f"[{color}]Traceback:[/{color}]")
                        syntax = Syntax(failure.traceback, "pytb", theme="ansi", line_numbers=False)
                        console.print(syntax)

                    console.print("=" * 80)
