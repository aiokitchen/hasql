import asyncio
from contextlib import AsyncExitStack

import mock
import pytest
from psycopg import AsyncConnection
from psycopg_pool import TooManyRequests

from hasql.metrics import DriverMetrics
from hasql.psycopg3 import PoolManager


@pytest.fixture
def pool_size() -> int:
    return 10


@pytest.fixture
async def pool_manager(pg_dsn, pool_size):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        acquire_timeout=1,
        pool_factory_kwargs={"min_size": pool_size, "max_size": pool_size},
    )
    try:
        await pg_pool.ready()
        yield pg_pool
    finally:
        await pg_pool.close()


async def test_acquire_with_context(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert isinstance(conn, AsyncConnection)
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            assert await cursor.fetchall() == [(1,)]


async def test_acquire_without_context(pool_manager):
    conn = await pool_manager.acquire_master()
    assert isinstance(conn, AsyncConnection)
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        assert await cursor.fetchall() == [(1,)]


async def test_close(pool_manager):
    aiopg_pool = await pool_manager.balancer.get_pool(read_only=False)
    await pool_manager.close()
    assert aiopg_pool.closed


async def test_release(pool_manager):
    aiopg_pool = await pool_manager.balancer.get_pool(read_only=False)
    assert pool_manager.get_pool_freesize(aiopg_pool) == 10
    conn = await pool_manager.acquire_master()
    assert pool_manager.get_pool_freesize(aiopg_pool) == 9
    await pool_manager.release(conn)
    assert pool_manager.get_pool_freesize(aiopg_pool) == 10


async def test_is_connection_closed(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert not pool_manager.is_connection_closed(conn)
        await conn.close()
        assert pool_manager.is_connection_closed(conn)


async def test_acquire_with_timeout_context(pool_manager, pool_size):
    conns = []
    for _ in range(pool_size):
        conns.append(await pool_manager.acquire_master())

    with pytest.raises(TooManyRequests):
        await pool_manager.acquire_master()

    for conn in conns:
        await pool_manager.release(conn)
    conns.clear()

    for pool in pool_manager.pools:
        assert pool_manager.get_pool_freesize(pool) == pool_size

    for _ in range(pool_size):
        async with pool_manager.acquire_master() as conn:
            pass


async def test_acquire_with_timeout_context2(pool_manager, pool_size):
    async with AsyncExitStack() as stack:
        for _ in range(pool_size):
            await stack.enter_async_context(pool_manager.acquire_master())

            async def wait_for_smth():
                with pytest.raises(TooManyRequests):
                    async with pool_manager.acquire_master():
                        pass

        await asyncio.gather(*[wait_for_smth() for _ in range(pool_size)])

    for pool in pool_manager.pools:
        assert pool_manager.get_pool_freesize(pool) == pool_size

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(pool_manager.acquire_master())


async def test_metrics(pool_manager):
    async with pool_manager.acquire_master():
        assert pool_manager.metrics().drivers == [
            DriverMetrics(max=11, min=11, idle=9, used=11, host=mock.ANY)
        ]
