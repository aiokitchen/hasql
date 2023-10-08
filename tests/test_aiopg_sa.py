import mock
import pytest
from aiopg.sa import SAConnection

from hasql.aiopg_sa import PoolManager
from hasql.metrics import DriverMetrics


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(dsn=pg_dsn, fallback_master=True)
    try:
        await pg_pool.ready()
        yield pg_pool
    finally:
        await pg_pool.close()


async def test_acquire_with_context(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert isinstance(conn, SAConnection)
        cursor = await conn.execute("SELECT 1")
        assert await cursor.fetchall() == [(1,)]


async def test_acquire_without_context(pool_manager):
    conn = await pool_manager.acquire_master()
    assert isinstance(conn, SAConnection)
    cursor = await conn.execute("SELECT 1")
    assert await cursor.fetchall() == [(1,)]


async def test_metrics(pool_manager):
    async with pool_manager.acquire_master():
        assert pool_manager.metrics().drivers == [
            DriverMetrics(max=11, min=2, idle=0, used=2, host=mock.ANY)
        ]
