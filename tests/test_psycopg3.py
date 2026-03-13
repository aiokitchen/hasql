import asyncio
from contextlib import AsyncExitStack

import mock
import pytest
pytest.importorskip("psycopg")
pytest.importorskip("psycopg_pool")
from psycopg import AsyncConnection
from psycopg_pool import PoolTimeout, TooManyRequests

from hasql.driver.psycopg3 import PoolManager


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
        await pg_pool.pool_state.ready()
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
    assert pool_manager.pool_state.get_pool_freesize(aiopg_pool) == 10
    conn = await pool_manager.acquire_master()
    assert pool_manager.pool_state.get_pool_freesize(aiopg_pool) == 9
    await pool_manager.release(conn)
    assert pool_manager.pool_state.get_pool_freesize(aiopg_pool) == 10


async def test_is_connection_closed(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert not pool_manager.is_connection_closed(conn)
        await conn.close()
        assert pool_manager.is_connection_closed(conn)


async def test_acquire_with_timeout_context(pool_manager, pool_size):
    conns = []
    for _ in range(pool_size):
        conns.append(await pool_manager.acquire_master())

    with pytest.raises(PoolTimeout):
        await pool_manager.acquire_master()

    for conn in conns:
        await pool_manager.release(conn)
    conns.clear()

    for pool in pool_manager.pool_state.pools:
        assert pool_manager.pool_state.get_pool_freesize(pool) == pool_size

    for _ in range(pool_size):
        async with pool_manager.acquire_master() as conn:
            pass


@pytest.fixture
async def queue_limited_pool_manager(pg_dsn, pool_size):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        acquire_timeout=1,
        pool_factory_kwargs={
            "min_size": pool_size,
            "max_size": pool_size,
            "max_waiting": 1,
        },
    )
    try:
        await pg_pool.pool_state.ready()
        yield pg_pool
    finally:
        await pg_pool.close()


async def test_acquire_with_queue_limit(queue_limited_pool_manager, pool_size):
    async with AsyncExitStack() as stack:
        for _ in range(pool_size):
            await stack.enter_async_context(
                queue_limited_pool_manager.acquire_master(),
            )

        async def wait_for_connection():
            conn = await queue_limited_pool_manager.acquire_master()
            await queue_limited_pool_manager.release(conn)

        waiter = asyncio.create_task(wait_for_connection())
        await asyncio.sleep(0.1)

        with pytest.raises(TooManyRequests):
            await queue_limited_pool_manager.acquire_master()

        assert not waiter.done()

    await waiter


def test_acquire_from_pool_passes_timeout():
    from hasql.driver.psycopg3 import Psycopg3AcquireContext, Psycopg3Driver

    pool_manager = PoolManager.__new__(PoolManager)
    pool_manager._driver = Psycopg3Driver()
    pool = mock.MagicMock()
    ctx = pool_manager.acquire_from_pool(pool, timeout=0.25)
    assert isinstance(ctx, Psycopg3AcquireContext)
    assert ctx.timeout == 0.25


async def test_metrics(pool_manager):
    async with pool_manager.acquire_master():
        pools = pool_manager.metrics().pools
        assert len(pools) == 1
        p = pools[0]
        assert p.max == 11
        assert p.min == 11
        assert p.idle == 9
        assert p.used == 2
        assert p.role == "master"
        assert p.healthy is True
        assert p.in_flight == 1
        assert "pool_size" in p.extra
