import pytest
from mock import mock

from hasql.metrics import HasqlMetrics
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
async def test_hasql_context_metrics(pool_manager_factory, pg_dsn):
    async with pool_manager_factory(pg_dsn) as pool_manager:
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
async def test_hasql_metrics(pool_manager_factory, pg_dsn):
    async with pool_manager_factory(pg_dsn) as pool_manager:
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
async def test_hasql_close_metrics(pool_manager_factory, pg_dsn):
    async with pool_manager_factory(pg_dsn) as pool_manager:
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
