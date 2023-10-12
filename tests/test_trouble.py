import asyncio
from unittest import mock

import pytest
from async_timeout import timeout

from tests.conftest import setup_aiopg, setup_aiopgsa, setup_asyncpg, setup_asyncsqlalchemy, setup_psycopg3


@pytest.mark.parametrize(
    "pool_manager_factory",
    [
        (setup_aiopg),
        (setup_aiopgsa),
        (setup_asyncpg),
        (setup_asyncsqlalchemy),
        (setup_psycopg3),
    ]
)
async def test_unavailable_db(pool_manager_factory, localhost, db_server_port):
    async with timeout(1):
        pg_dsn = f"postgres://pg:pg@{localhost}:{db_server_port}/pg"
        async with pool_manager_factory(pg_dsn) as pool_manager:
            pass


@pytest.mark.parametrize(
    "pool_manager_factory,name",
    [
        (setup_aiopg, "aiopg"),
        (setup_aiopgsa, "aiopg_sa"),
        (setup_asyncpg, "asyncpg"),
        (setup_asyncsqlalchemy, "asyncsqlalchemy"),
        (setup_psycopg3, "psycopg3"),
    ]
)
async def test_catch_cancelled_error(pool_manager_factory, pg_dsn, name):
    async with pool_manager_factory(pg_dsn) as pool_manager:
        await pool_manager.ready()
        assert pool_manager.available_pool_count > 0
        with mock.patch(
            f"hasql.{name}.PoolManager._is_master",
            side_effect=asyncio.CancelledError(),
        ):
            await pool_manager.wait_next_pool_check()
            assert pool_manager.available_pool_count == 0
        await pool_manager.wait_next_pool_check()
        assert pool_manager.available_pool_count > 0
