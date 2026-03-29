import mock
import pytest
from asyncpg import Connection

from hasql.driver.asyncpg import PoolManager
from hasql.metrics import PoolStats


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        pool_factory_kwargs={"min_size": 10, "max_size": 10},
    )
    try:
        await pg_pool._pool_state.ready()
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
    asyncpg_pool = await pool_manager._balancer.get_pool(read_only=False)
    await pool_manager.close()
    assert asyncpg_pool._closed


async def test_release(pool_manager):
    asyncpg_pool = await pool_manager._balancer.get_pool(read_only=False)
    assert pool_manager._pool_state.get_pool_freesize(asyncpg_pool) == 10
    async with pool_manager.acquire_master() as _conn:
        assert pool_manager._pool_state.get_pool_freesize(asyncpg_pool) == 9
    assert pool_manager._pool_state.get_pool_freesize(asyncpg_pool) == 10


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


def test_acquire_from_pool_passes_timeout():
    from hasql.driver.asyncpg import AsyncpgDriver

    driver = AsyncpgDriver()
    pool = mock.MagicMock()
    driver.acquire_from_pool(pool, timeout=0.25)
    pool.acquire.assert_called_once_with(timeout=0.25)


def test_asyncpg_version_parsing_release():
    """Standard release version parses correctly."""
    from hasql.driver.asyncpg import _asyncpg_version

    version = _asyncpg_version()
    assert isinstance(version, tuple)
    assert all(isinstance(x, int) for x in version)
    assert len(version) <= 3


def test_asyncpg_version_parsing_prerelease(monkeypatch):
    """Pre-release versions like '0.29.0rc1' don't crash."""
    import asyncpg

    from hasql.driver.asyncpg import _asyncpg_version

    monkeypatch.setattr(asyncpg, "__version__", "0.29.0rc1")
    version = _asyncpg_version()
    assert version == (0, 29, 0)


def test_asyncpg_version_parsing_dev(monkeypatch):
    """Dev versions like '0.30.0.dev0' don't crash."""
    import asyncpg

    from hasql.driver.asyncpg import _asyncpg_version

    monkeypatch.setattr(asyncpg, "__version__", "0.30.0.dev0")
    version = _asyncpg_version()
    assert version == (0, 30, 0)


def test_pool_stats_dynamic_pool_size():
    """used should be based on actual pool size, not maxsize."""
    from hasql.driver.asyncpg import AsyncpgDriver

    driver = AsyncpgDriver()
    pool = mock.MagicMock()
    pool._maxsize = 20
    pool._minsize = 5
    pool.get_size.return_value = 8  # only 8 connections created so far
    pool._queue.qsize.return_value = 5  # 5 idle

    stats = driver.pool_stats(pool)
    assert stats == PoolStats(max=20, min=5, idle=5, used=3)
