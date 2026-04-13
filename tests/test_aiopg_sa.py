import pytest
from aiopg.sa import SAConnection

from hasql.driver.aiopg_sa import PoolManager


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(dsn=pg_dsn, fallback_master=True)
    try:
        await pg_pool._pool_state.ready()
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
        pools = pool_manager.metrics().pools
        assert len(pools) == 1
        p = pools[0]
        assert p.max == 11
        assert p.min == 2
        assert p.idle == 0
        assert p.used == 2
        assert p.role == "master"
        assert p.healthy is True
        assert p.in_flight == 1
