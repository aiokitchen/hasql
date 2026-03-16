import asyncio
from unittest import mock

import pytest
from async_timeout import timeout

from tests.conftest import (
    setup_aiopg,
    setup_aiopgsa,
    setup_asyncpg,
    setup_asyncsqlalchemy,
    setup_psycopg3,
)


@pytest.mark.parametrize(
    "pool_manager_factory",
    [
        (setup_aiopg),
        (setup_aiopgsa),
        (setup_asyncpg),
        (setup_asyncsqlalchemy),
        (setup_psycopg3),
    ],
)
async def test_unavailable_db(pool_manager_factory, localhost, db_server_port):
    async with timeout(1):
        pg_dsn = f"postgres://pg:pg@{localhost}:{db_server_port}/pg"
        async with pool_manager_factory(pg_dsn):
            pass


_AIOSQA = "hasql.driver.asyncsqlalchemy.AsyncSqlAlchemyDriver"


@pytest.mark.parametrize(
    "pool_manager_factory,driver_class",
    [
        (setup_aiopg, "hasql.driver.aiopg.AiopgDriver"),
        (setup_aiopgsa, "hasql.driver.aiopg_sa.AiopgSaDriver"),
        (setup_asyncpg, "hasql.driver.asyncpg.AsyncpgDriver"),
        (setup_asyncsqlalchemy, _AIOSQA),
        (setup_psycopg3, "hasql.driver.psycopg3.Psycopg3Driver"),
    ],
)
async def test_catch_cancelled_error(
    pool_manager_factory, pg_dsn, driver_class,
):
    async with pool_manager_factory(pg_dsn) as pool_manager:
        await pool_manager._pool_state.ready()
        assert pool_manager._pool_state.available_pool_count > 0
        with mock.patch(
            f"{driver_class}.is_master",
            side_effect=asyncio.CancelledError(),
        ):
            await pool_manager._pool_state.wait_next_pool_check()
            assert pool_manager._pool_state.available_pool_count == 0
        await pool_manager._pool_state.wait_next_pool_check()
        assert pool_manager._pool_state.available_pool_count > 0
