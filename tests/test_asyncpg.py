import mock
import pytest
from asyncpg import Connection

from hasql.asyncpg import PoolManager
from hasql.metrics import Metrics


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        pool_factory_kwargs={"min_size": 10, "max_size": 10},
    )
    try:
        await pg_pool.ready()
        yield pg_pool
    finally:
        await pg_pool.close()


async def test_acquire_with_context(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert isinstance(conn, Connection)
        assert await conn.fetch("SELECT 1") == [(1,)]


async def test_acquire_without_context(pool_manager):
    conn = await pool_manager.acquire_master()
    assert isinstance(conn, Connection)
    assert await conn.fetch("SELECT 1") == [(1,)]


async def test_close(pool_manager):
    asyncpg_pool = await pool_manager.balancer.get_pool(read_only=False)
    await pool_manager.close()
    assert asyncpg_pool._closed


async def test_terminate(pool_manager):
    asyncpg_pool = await pool_manager.balancer.get_pool(read_only=False)
    await pool_manager.terminate()
    assert asyncpg_pool._closed


async def test_release(pool_manager):
    asyncpg_pool = await pool_manager.balancer.get_pool(read_only=False)
    assert pool_manager.get_pool_freesize(asyncpg_pool) == 10
    conn = await pool_manager.acquire_master()
    assert pool_manager.get_pool_freesize(asyncpg_pool) == 9
    await pool_manager.release(conn)
    assert pool_manager.get_pool_freesize(asyncpg_pool) == 10


async def test_metrics(pool_manager):
    async with pool_manager.acquire_master():
        assert pool_manager.metrics() == [
            Metrics(max=11, min=11, idle=9, used=2, host=mock.ANY)
        ]
