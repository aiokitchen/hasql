import mock
import pytest
from aiopg import Connection

from hasql.driver.aiopg import PoolManager


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        pool_factory_kwargs={"minsize": 10, "maxsize": 10},
    )
    try:
        yield pg_pool
    finally:
        await pg_pool.close()


async def test_acquire_with_context(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert isinstance(conn, Connection)
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            assert await cursor.fetchall() == [(1,)]


async def test_acquire_without_context(pool_manager):
    conn = await pool_manager.acquire_master()
    assert isinstance(conn, Connection)
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        assert await cursor.fetchall() == [(1,)]


async def test_close(pool_manager):
    aiopg_pool = await pool_manager._balancer.get_pool(read_only=False)
    await pool_manager.close()
    assert aiopg_pool.closed


async def test_release(pool_manager):
    aiopg_pool = await pool_manager._balancer.get_pool(read_only=False)
    assert pool_manager._pool_state.get_pool_freesize(aiopg_pool) == 10
    async with pool_manager.acquire_master() as _conn:
        assert pool_manager._pool_state.get_pool_freesize(aiopg_pool) == 9
    assert pool_manager._pool_state.get_pool_freesize(aiopg_pool) == 10


async def test_is_connection_closed(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert not pool_manager._pool_state.is_connection_closed(conn)
        await conn.close()
        assert pool_manager._pool_state.is_connection_closed(conn)


async def test_driver_context_metrics(pool_manager, pg_dsn):
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


async def test_driver_metrics(pool_manager, pg_dsn):
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


def test_acquire_from_pool_wraps_with_timeout():
    from hasql.acquire import TimeoutAcquireContext
    from hasql.driver.aiopg import AiopgDriver

    driver = AiopgDriver()
    pool = mock.MagicMock()
    ctx = driver.acquire_from_pool(pool, timeout=0.25)
    assert isinstance(ctx, TimeoutAcquireContext)


def test_acquire_from_pool_no_timeout():
    from hasql.acquire import TimeoutAcquireContext
    from hasql.driver.aiopg import AiopgDriver

    driver = AiopgDriver()
    pool = mock.MagicMock()
    ctx = driver.acquire_from_pool(pool)
    assert not isinstance(ctx, TimeoutAcquireContext)
