import mock
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from hasql.asyncsqlalchemy import PoolManager
from hasql.metrics import Metrics


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        pool_factory_kwargs={"pool_size": 10},
    )
    try:
        await pg_pool.ready()
        yield pg_pool
        pass
    finally:
        await pg_pool.close()


async def test_acquire_with_context(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert isinstance(conn, AsyncConnection)
        assert await conn.scalar(sa.text("SELECT 1")) == 1


async def test_acquire_without_context(pool_manager):
    conn = await pool_manager.acquire_master()
    assert isinstance(conn, AsyncConnection)
    assert await conn.scalar(sa.text("SELECT 1")) == 1


async def test_close(pool_manager):
    sqlalchemy_pool: AsyncEngine = await pool_manager.balancer.get_pool(
        read_only=False,
    )
    assert sqlalchemy_pool.sync_engine.pool.checkedout() > 0
    await pool_manager.close()
    assert sqlalchemy_pool.sync_engine.pool.checkedout() == 0


async def test_terminate(pool_manager):
    sqlalchemy_pool: AsyncEngine = await pool_manager.balancer.get_pool(
        read_only=False,
    )
    assert sqlalchemy_pool.sync_engine.pool.overflow() == -10
    await pool_manager.terminate()
    assert sqlalchemy_pool.sync_engine.pool.overflow() == -11


async def test_release(pool_manager):
    sqlalchemy_pool = await pool_manager.balancer.get_pool(read_only=False)
    assert pool_manager.get_pool_freesize(sqlalchemy_pool) == 10
    conn = await pool_manager.acquire_master()
    assert pool_manager.get_pool_freesize(sqlalchemy_pool) == 9
    await pool_manager.release(conn)
    assert pool_manager.get_pool_freesize(sqlalchemy_pool) == 10


async def test_is_connection_closed(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert not pool_manager.is_connection_closed(conn)
        await conn.close()
        assert pool_manager.is_connection_closed(conn)


async def test_metrics(pool_manager):
    async with pool_manager.acquire_master():
        assert pool_manager.metrics() == [
            Metrics(max=11, min=0, idle=0, used=2, host=mock.ANY)
        ]
