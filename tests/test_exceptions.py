from unittest.mock import AsyncMock, MagicMock

import pytest

from hasql.driver.psycopg3 import Psycopg3Driver
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


class TestSuiteBaseModuleExports:
    def test_import_hasql_error_from_base(self):
        from hasql.base import HasqlError as HasqlErrorFromBase

        assert HasqlErrorFromBase is HasqlError

    def test_import_pool_manager_closed_error_from_base(self):
        from hasql.base import PoolManagerClosedError as FromBase

        assert FromBase is PoolManagerClosedError

    def test_import_no_available_pool_error_from_base(self):
        from hasql.base import NoAvailablePoolError as FromBase

        assert FromBase is NoAvailablePoolError

    def test_import_unexpected_database_response_error_from_base(self):
        from hasql.base import UnexpectedDatabaseResponseError as FromBase

        assert FromBase is UnexpectedDatabaseResponseError


class TestSuitePsycopg3DriverIsMaster:
    async def test_is_master_raises_on_none_row(self):
        driver = Psycopg3Driver()
        cursor = AsyncMock()
        cursor.fetchone = AsyncMock(return_value=None)
        connection = MagicMock()
        connection.cursor = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=cursor),
                __aexit__=AsyncMock(return_value=False),
            ),
        )
        with pytest.raises(UnexpectedDatabaseResponseError):
            await driver.is_master(connection)

    async def test_unexpected_database_response_error_message_contains_show(
        self,
    ):
        driver = Psycopg3Driver()
        cursor = AsyncMock()
        cursor.fetchone = AsyncMock(return_value=None)
        connection = MagicMock()
        connection.cursor = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=cursor),
                __aexit__=AsyncMock(return_value=False),
            ),
        )
        with pytest.raises(
            UnexpectedDatabaseResponseError,
            match="SHOW transaction_read_only",
        ):
            await driver.is_master(connection)
