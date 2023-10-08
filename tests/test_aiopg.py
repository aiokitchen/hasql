import mock
import pytest
from aiopg import Connection

from hasql.aiopg import PoolManager
from hasql.metrics import DriverMetrics, HasqlMetrics


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


async def test_driver_context_metrics(pool_manager, pg_dsn):
    async with pool_manager.acquire_master():
        assert pool_manager.metrics().drivers == [
            DriverMetrics(max=11, min=11, idle=9, used=2, host=mock.ANY)
        ]


async def test_driver_metrics(pool_manager, pg_dsn):
    _ = await pool_manager.acquire_master()
    assert pool_manager.metrics().drivers == [
        DriverMetrics(max=11, min=11, idle=9, used=2, host=mock.ANY)
    ]


async def test_hasql_context_metrics(pool_manager):
    async with pool_manager.acquire_master():
        metrics = pool_manager.metrics().hasql
        assert metrics == HasqlMetrics(
            pool=1,
            acquire=1,
            pool_time=mock.ANY,
            acquire_time=mock.ANY,
            add_connections=mock.ANY,
            remove_connections=mock.ANY,
        )
        assert list(metrics.add_connections.values()) == [1]
        assert metrics.remove_connections == {}

    metrics = pool_manager.metrics().hasql
    assert metrics == HasqlMetrics(
        pool=1,
        acquire=1,
        pool_time=mock.ANY,
        acquire_time=mock.ANY,
        add_connections=mock.ANY,
        remove_connections=mock.ANY,
    )
    assert list(metrics.add_connections.values()) == [1]
    assert list(metrics.remove_connections.values()) == [1]


async def test_hasql_metrics(pool_manager: PoolManager):
    _conn = await pool_manager.acquire_master()
    metrics = pool_manager.metrics().hasql
    assert metrics == HasqlMetrics(
        pool=1,
        acquire=1,
        pool_time=mock.ANY,
        acquire_time=mock.ANY,
        add_connections=mock.ANY,
        remove_connections=mock.ANY,
    )
    assert list(metrics.add_connections.values()) == [1]
    assert metrics.remove_connections == {}

    await pool_manager.release(connection=_conn)

    metrics = pool_manager.metrics().hasql
    assert metrics == HasqlMetrics(
        pool=1,
        acquire=1,
        pool_time=mock.ANY,
        acquire_time=mock.ANY,
        add_connections=mock.ANY,
        remove_connections=mock.ANY,
    )
    assert list(metrics.add_connections.values()) == [1]
    assert list(metrics.remove_connections.values()) == [1]


async def test_hasql_close_metrics(pool_manager: PoolManager):
    _ = await pool_manager.acquire_master()
    await pool_manager.close()

    metrics = pool_manager.metrics().hasql
    assert metrics == HasqlMetrics(
        pool=1,
        acquire=1,
        pool_time=mock.ANY,
        acquire_time=mock.ANY,
        add_connections=mock.ANY,
        remove_connections=mock.ANY,
    )
    assert list(metrics.add_connections.values()) == [1]
    assert list(metrics.remove_connections.values()) == [1]
