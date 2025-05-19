import pytest
import inspect
from typing import Generator

from aind_behavior_core_analysis.qc._base import (
    TestSuite, TestStatus, TestResult, TestStatistics, 
    implicit_pass, allow_null_as_pass, allow_skippable
)


class SimpleTestSuite(TestSuite):
    """A simple test suite for testing the TestSuite class."""
    
    def test_always_pass(self):
        """A test that always passes."""
        return self.pass_test("pass result", "This test passed")
    
    def test_always_fail(self):
        """A test that always fails."""
        return self.fail_test("fail result", "This test failed")
    
    def test_always_skip(self):
        """A test that always skips."""
        return self.skip_test("This test was skipped")
    
    def test_return_none(self):
        """A test that returns None."""
        return None

    def test_yielding_results(self):
        """A test that yields multiple results."""
        yield self.pass_test("first", "First yielded test")
        yield self.pass_test("second", "Second yielded test")
        yield self.fail_test("third", "Third yielded test")

    @implicit_pass
    def test_implicit_pass(self):
        """A test using the implicit_pass decorator."""
        return "This should be auto-converted to a pass"
    
    def test_implicit_fail(self):
        """A test that fails because it returns None and is not decorated."""
        return "I should fail"
    
    def not_a_test(self):
        """This is not a test method."""
        return "Not a test"


class TestTestSuite:
    """Tests for the TestSuite class."""
    
    def test_get_tests(self):
        """Test that get_tests returns all test methods."""
        suite = SimpleTestSuite()
        tests = list(suite.get_tests())
        
        # The number of tests in the SimpleTestSuite
        assert len(tests) == 7
        
        for test in tests:
            assert callable(test)
            
        # Should only include methods starting with 'test_'
        test_names = [test.__name__ for test in tests]
        assert 'not_a_test' not in test_names
        assert 'test_always_pass' in test_names
    
    def test_run_test_pass(self):
        """Test running a test that passes."""
        suite = SimpleTestSuite()
        test_method = suite.test_always_pass
        results = list(suite.run_test(test_method))
        
        assert len(results) == 1
        assert results[0].status == TestStatus.PASSED
        assert results[0].result == "pass result"
        assert results[0].message == "This test passed"
    
    def test_run_test_fail(self):
        """Test running a test that fails."""
        suite = SimpleTestSuite()
        test_method = suite.test_always_fail
        results = list(suite.run_test(test_method))
        
        assert len(results) == 1
        assert results[0].status == TestStatus.FAILED
        assert results[0].result == "fail result"
        assert results[0].message == "This test failed"
    
    def test_run_test_skip(self):
        """Test running a test that skips."""
        suite = SimpleTestSuite()
        test_method = suite.test_always_skip

        with allow_skippable(True):
            results = list(suite.run_test(test_method))
        
        assert len(results) == 1
        assert results[0].status == TestStatus.SKIPPED
        assert results[0].message == "This test was skipped"
        
    def test_run_test_skip_not_allowed(self):
        """Test running a test that skips in a non-skippable context."""
        suite = SimpleTestSuite()
        test_method = suite.test_always_skip

        with allow_skippable(False):
            results = list(suite.run_test(test_method))
        
        assert len(results) == 1
        assert results[0].status == TestStatus.FAILED
        assert results[0].message == "This test was skipped"
    
    def test_run_test_none(self):
        """Test running a test that returns None."""
        suite = SimpleTestSuite()
        test_method = suite.test_return_none
        
        with allow_null_as_pass(value=True):
            results = list(suite.run_test(test_method))
            assert len(results) == 1
            assert results[0].status == TestStatus.PASSED
        
    def test_run_test_none_not_allowed(self):
        suite = SimpleTestSuite()
        test_method = suite.test_return_none
        with allow_null_as_pass(value=False):
            results = list(suite.run_test(test_method))
            assert len(results) == 1
            assert results[0].status == TestStatus.ERROR
    
    def test_run_test_yielding(self):
        """Test running a test that yields multiple results."""
        suite = SimpleTestSuite()
        test_method = suite.test_yielding_results
        results = list(suite.run_test(test_method))
        
        assert len(results) == 3
        assert results[0].status == TestStatus.PASSED
        assert results[1].status == TestStatus.PASSED
        assert results[2].status == TestStatus.FAILED
        
        assert results[0].result == "first"
        assert results[1].result == "second"
        assert results[2].result == "third"
    
    def test_run_test_with_implicit_pass(self):
        """Test running a test with the implicit_pass decorator."""
        suite = SimpleTestSuite()
        test_method = suite.test_implicit_pass
        results = list(suite.run_test(test_method))
        
        assert len(results) == 1
        assert results[0].status == TestStatus.PASSED
        assert "auto-converted" in results[0].message.lower()
        
    def test_run_test_implicit_fail(self):
        """Test running a test that fails because it returns None."""
        suite = SimpleTestSuite()
        test_method = suite.test_implicit_fail
        results = list(suite.run_test(test_method))
        
        assert len(results) == 1
        assert results[0].status == TestStatus.ERROR
        assert isinstance(results[0].exception, TypeError)
    
    def test_run_all(self):
        """Test running all tests in a suite."""
        suite = SimpleTestSuite()
        results = list(suite.run_all())
        
        assert len(results) == 9
        
        statuses = [r.status for r in results]
        assert statuses.count(TestStatus.PASSED) == 4
        assert statuses.count(TestStatus.FAILED) == 2 
        assert statuses.count(TestStatus.SKIPPED) == 1
        assert statuses.count(TestStatus.ERROR) == 2
    
    def test_run_all_with_context(self):
        """Test running all tests with context managers."""
        suite = SimpleTestSuite()
        
        with allow_null_as_pass():
            with allow_skippable(False):
                results = list(suite.run_all())
                
                statuses = [r.status for r in results]
                assert statuses.count(TestStatus.PASSED) == 5
                assert statuses.count(TestStatus.FAILED) == 3
                assert statuses.count(TestStatus.SKIPPED) == 0
                assert statuses.count(TestStatus.ERROR) == 1
    
    def test_setup_teardown(self):
        
        class SetupTeardownSuite(TestSuite):
            def __init__(self):
                self.setup_called = 0
                self.teardown_called = 0
                self.test_called = 0
            
            def setup(self):
                self.setup_called += 1
            
            def teardown(self):
                self.teardown_called += 1
            
            def test_something(self):
                self.test_called += 1
                return self.pass_test()
        
            def test_something_with_yield(self):
                self.test_called += 1
                for i in range(3):
                    yield self.pass_test()
        
        suite = SetupTeardownSuite()
        list(suite.run_all())
        
        assert suite.setup_called == 2
        assert suite.teardown_called == 2
        assert suite.test_called == 2
    
    def test_teardown_error(self):
        """Test that teardown errors are properly reported."""
        
        class ErrorTeardownSuite(TestSuite):
            def test_something(self):
                return self.pass_test()
            
            def teardown(self):
                raise ValueError("Teardown error")
        
        suite = ErrorTeardownSuite()
        with pytest.raises(ValueError) as e_info:
            results = list(suite.run_all())
        assert "Teardown error" in str(e_info.value)
