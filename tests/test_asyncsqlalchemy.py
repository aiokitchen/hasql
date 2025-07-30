from typing import Optional, Type

import mock
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from hasql.asyncsqlalchemy import PoolManager, async_sessionmaker
from hasql.metrics import DriverMetrics


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
        assert pool_manager.metrics().drivers == [
            DriverMetrics(max=11, min=0, idle=0, used=2, host=mock.ANY)
        ]


@pytest.mark.parametrize("expire_on_commit", [True, False, None])
@pytest.mark.parametrize("autoflush", [True, False, None])
@pytest.mark.parametrize("read_only", [True, False, None])
@pytest.mark.parametrize("class_", [AsyncSession, None])
async def test_async_sessionmaker(
    pool_manager: PoolManager,
    expire_on_commit: Optional[bool],
    autoflush: Optional[bool],
    read_only: Optional[bool],
    class_: Optional[Type[AsyncSession]],
):
    acquire_kwargs = {}

    if read_only is not None:
        acquire_kwargs["read_only"] = read_only

    kwargs = {}

    if expire_on_commit is not None:
        kwargs["expire_on_commit"] = expire_on_commit

    if autoflush is not None:
        kwargs["autoflush"] = autoflush

    if class_ is not None:
        kwargs["class_"] = class_

    if acquire_kwargs:
        kwargs["acquire_kwargs"] = acquire_kwargs

    session_factory = async_sessionmaker(pool_manager=pool_manager, **kwargs)

    async with session_factory() as session:  # type: AsyncSession
        result = await session.execute(sa.text("SELECT 1"))
        assert result.scalar() == 1

        if expire_on_commit is not None:
            assert session.sync_session.expire_on_commit == expire_on_commit

        if autoflush is not None:
            assert session.sync_session.autoflush == autoflush

        if class_ is not None:
            assert isinstance(session, class_)
