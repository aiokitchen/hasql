import asyncio
from unittest import mock

import pytest
from async_timeout import timeout

from hasql.aiopg import PoolManager


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        pool_factory_kwargs={"minsize": 10, "maxsize": 10,},
    )
    try:
        await pg_pool.ready()
        yield pg_pool
    finally:
        await pg_pool.close()


async def test_unavailable_db(localhost, db_server_port):
    async with timeout(1):
        pg_dsn = f"postgres://pg:pg@{localhost}:{db_server_port}/pg"
        pg_pool = PoolManager(dsn=pg_dsn, fallback_master=True,)
        await pg_pool.close()


async def test_catch_cancelled_error(pool_manager):
    assert pool_manager.available_pool_count > 0
    with mock.patch(
        "hasql.aiopg.PoolManager._is_master", side_effect=asyncio.CancelledError(),
    ):
        await pool_manager.wait_next_pool_check()
        assert pool_manager.available_pool_count == 0
    await pool_manager.wait_next_pool_check()
    assert pool_manager.available_pool_count > 0
