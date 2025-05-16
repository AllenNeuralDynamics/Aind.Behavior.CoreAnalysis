import abc
import dataclasses
import inspect
import traceback
import typing
from enum import Enum
from typing import Annotated, Any, Dict, Generator, List, Optional, Protocol, TypeVar, get_args, get_origin

import rich.progress
from rich.console import Console
from rich.syntax import Syntax

_SKIPPABLE = True


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


class ITest(Protocol):
    def __call__(self) -> "TestResult":
        pass


@dataclasses.dataclass
class Annotation:
    message: Optional[str] = None
    context: Optional[Any] = None


class FailTest(Exception):
    @typing.overload
    def __init__(self, result: Optional[Any] = None) -> None: ...

    @typing.overload
    def __init__(self, result: Optional[Any] = None, annotation: str = None) -> None: ...

    @typing.overload
    def __init__(self, result: Optional[Any] = None, annotation: Annotation = None) -> None: ...

    def __init__(self, result: Optional[Any] = None, annotation: Optional[str | Annotation] = None):
        if isinstance(annotation, str):
            annotation = Annotation(message=annotation)
        self.result, self.annotation = TestSuite._unwrap_annotated(result)
        self.annotation = annotation or self.annotation
        super().__init__(self.annotation.message if self.annotation else None)


class SkipTest(Exception):
    @typing.overload
    def __init__(self) -> None: ...
    @typing.overload
    def __init__(self, annotation: str = None) -> None: ...
    @typing.overload
    def __init__(self, annotation: Annotation = None) -> None: ...

    def __init__(self, annotation: Optional[str | Annotation] = None):
        if isinstance(annotation, str):
            annotation = Annotation(message=annotation)
        self.annotation = annotation
        super().__init__(self.annotation.message if self.annotation else None)


@dataclasses.dataclass
class TestResult:
    status: TestStatus
    result: Any
    test_name: str
    suite_name: str
    _test_reference: Optional[ITest] = dataclasses.field(default=None, repr=False)
    message: Optional[str] = None
    context: Optional[Any] = dataclasses.field(default=None, repr=False)
    description: Optional[str] = dataclasses.field(default=None, repr=False)
    exception: Optional[Exception] = dataclasses.field(default=None, repr=False)
    traceback: Optional[str] = dataclasses.field(default=None, repr=False)


class TestSuite(abc.ABC):
    def get_tests(self) -> Generator[typing.Callable, None, None]:
        """Find all methods starting with 'test'."""
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if name.startswith("test"):
                yield method

    _T = TypeVar("_T")

    @staticmethod
    def _unwrap_annotated(result: _T) -> tuple[_T, Optional[Annotation]]:
        extra_data = None
        original_result = result
        if get_origin(result) is Annotated:
            args = get_args(result)
            if args and len(args) >= 2:
                original_result = args[0]
                for arg in args[1:]:
                    if isinstance(arg, Annotation):
                        extra_data = arg
                        break

        return original_result, extra_data

    def run_test(self, test_method: typing.Callable) -> TestResult:
        test_name = test_method.__name__
        suite_name = self.__class__.__name__
        description = getattr(test_method, "__doc__", None)

        try:
            result = test_method()
        except SkipTest as e:
            tb = traceback.format_exc()
            return TestResult(
                status=TestStatus.SKIPPED if _SKIPPABLE else TestStatus.FAILED,
                result=None,
                test_name=test_name,
                suite_name=suite_name,
                description=description,
                message=e.annotation.message,
                exception=e,
                traceback=tb,
                _test_reference=test_method,
            )
        except FailTest as e:
            tb = traceback.format_exc()
            return TestResult(
                status=TestStatus.FAILED,
                result=e.result,
                test_name=test_name,
                suite_name=suite_name,
                description=description,
                message=e.annotation.message,
                exception=e,
                traceback=tb,
                _test_reference=test_method,
            )
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
        else:
            annotation: Optional[Annotation]
            result, annotation = self._unwrap_annotated(result)

            if annotation:
                message = annotation.message
                context = annotation.context
            else:
                message = None
                context = None

            if isinstance(result, TestResult):
                return TestResult(
                    result.result,
                    status=result.status,
                    test_name=test_name,
                    suite_name=suite_name,
                    _test_reference=result._test_reference,
                    description=result.description or description,
                    message=result.message or message,
                    context=result.context or context,
                )

            return TestResult(
                status=TestStatus.PASSED,
                result=result,
                test_name=test_name,
                suite_name=suite_name,
                description=description,
                message=message,
                context=context,
                _test_reference=test_method,
            )

    def run_all(self) -> Generator[TestResult, None, None]:
        for test in self.get_tests():
            yield self.run_test(test)


class TestRunner:
    def __init__(self, suites: Optional[List[TestSuite]] = None):
        self.suites = suites if suites is not None else []
        self._results: Optional[List[TestResult]] = None

    def add_suite(self, suite: TestSuite) -> "TestRunner":
        self.suites.append(suite)
        return self

    def _calculate_statistics(self, results: List[TestResult]) -> Dict:
        total = len(results)
        stats = {status: sum(1 for r in results if r.status == status) for status in TestStatus}
        stats["total"] = total
        stats["pass_rate"] = (stats[TestStatus.PASSED] / total) if total > 0 else 0
        return stats

    def _render_status_bar(self, stats: Dict, bar_width: int = 20) -> str:
        """Render a colored status bar based on test statistics."""
        total = stats["total"]
        if total == 0:
            return ""

        status_bar = ""
        for status in TestStatus:
            if stats[status]:
                color = STATUS_COLOR[status]
                status_bar += f"[{color}]{'█' * int(bar_width * stats[status] / total)}[/{color}]"

        return status_bar

    def _get_status_summary(self, stats: Dict) -> str:
        return f"P:{stats[TestStatus.PASSED]} F:{stats[TestStatus.FAILED]} E:{stats[TestStatus.ERROR]} S:{stats[TestStatus.SKIPPED]}"

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
            rich.progress.TimeRemainingColumn(),
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
                    stats = self._calculate_statistics(suite_results)
                    status_bar = self._render_status_bar(stats, bar_width)
                    summary = self._get_status_summary(stats)

                    summary_line = f"[cyan]{suite_name:<{suite_name_width}} | {status_bar} | {summary}"
                    progress.update(suite_task, description=summary_line)

                all_results.extend(suite_results)

            if test_count > 0:
                total_stats = self._calculate_statistics(all_results)
                total_status_bar = self._render_status_bar(total_stats, bar_width)
                total_summary = self._get_status_summary(total_stats)

                total_line = f"[bold green]Total{' ':{suite_name_width - 5}} | {total_status_bar} | {total_summary}"
                progress.update(total_task, description=total_line)

        self._results = all_results
        if self._results:
            self.print_results(self._results)

        return all_results

    @staticmethod
    def print_results(
        all_results: List[TestResult], include: set[TestStatus] = (TestStatus.FAILED, TestStatus.ERROR)
    ) -> Optional[List[TestResult]]:
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
