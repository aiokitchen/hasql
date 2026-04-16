import pytest

from hasql.exceptions import (
    HasqlError,
    NoAvailablePoolError,
    PoolManagerClosedError,
    PoolManagerClosingError,
    UnexpectedDatabaseResponseError,
)


class TestSuiteExceptionHierarchy:
    def test_hasql_error_is_exception(self):
        assert issubclass(HasqlError, Exception)

    def test_pool_manager_closed_error_is_hasql_error(self):
        assert issubclass(PoolManagerClosedError, HasqlError)

    def test_no_available_pool_error_is_hasql_error(self):
        assert issubclass(NoAvailablePoolError, HasqlError)

    def test_unexpected_database_response_error_is_hasql_error(self):
        assert issubclass(UnexpectedDatabaseResponseError, HasqlError)

    def test_pool_manager_closing_error_is_hasql_error(self):
        assert issubclass(PoolManagerClosingError, HasqlError)


class TestSuiteExceptionMessages:
    def test_hasql_error_message(self):
        exc = HasqlError("test message")
        assert str(exc) == "test message"

    def test_pool_manager_closed_error_message(self):
        exc = PoolManagerClosedError("Pool manager is closed")
        assert str(exc) == "Pool manager is closed"

    def test_no_available_pool_error_message(self):
        exc = NoAvailablePoolError("No available pool")
        assert str(exc) == "No available pool"

    def test_unexpected_database_response_error_message(self):
        exc = UnexpectedDatabaseResponseError("Expected a row")
        assert str(exc) == "Expected a row"

    def test_pool_manager_closing_error_message(self):
        exc = PoolManagerClosingError("shutting down")
        assert str(exc) == "shutting down"


class TestSuiteCatchByBaseClass:
    def test_catch_pool_manager_closed_error_as_hasql_error(self):
        with pytest.raises(HasqlError):
            raise PoolManagerClosedError("closed")

    def test_catch_no_available_pool_error_as_hasql_error(self):
        with pytest.raises(HasqlError):
            raise NoAvailablePoolError("no pool")

    def test_catch_unexpected_database_response_error_as_hasql_error(self):
        with pytest.raises(HasqlError):
            raise UnexpectedDatabaseResponseError("unexpected")

    def test_catch_pool_manager_closing_error_as_hasql_error(self):
        with pytest.raises(HasqlError):
            raise PoolManagerClosingError("closing")
