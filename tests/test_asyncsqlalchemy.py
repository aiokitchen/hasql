import mock
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from hasql.driver.asyncsqlalchemy import PoolManager, async_sessionmaker


def test_acquire_from_pool_wraps_with_timeout():
    from hasql.acquire import TimeoutAcquireContext
    from hasql.driver.asyncsqlalchemy import AsyncSqlAlchemyDriver

    driver = AsyncSqlAlchemyDriver()
    pool = mock.MagicMock()
    ctx = driver.acquire_from_pool(pool, timeout=0.25)
    assert isinstance(ctx, TimeoutAcquireContext)


def test_acquire_from_pool_no_timeout():
    from hasql.acquire import TimeoutAcquireContext
    from hasql.driver.asyncsqlalchemy import AsyncSqlAlchemyDriver

    driver = AsyncSqlAlchemyDriver()
    pool = mock.MagicMock()
    ctx = driver.acquire_from_pool(pool)
    assert not isinstance(ctx, TimeoutAcquireContext)


@pytest.fixture
async def pool_manager(pg_dsn):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        pool_factory_kwargs={"pool_size": 10},
    )
    try:
        await pg_pool._pool_state.ready()
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
    sqlalchemy_pool: AsyncEngine = await pool_manager._balancer.get_pool(
        read_only=False,
    )
    assert sqlalchemy_pool.sync_engine.pool.checkedout() > 0
    await pool_manager.close()
    assert sqlalchemy_pool.sync_engine.pool.checkedout() == 0


async def test_release(pool_manager):
    sqlalchemy_pool = await pool_manager._balancer.get_pool(read_only=False)
    assert pool_manager._pool_state.get_pool_freesize(sqlalchemy_pool) == 10
    async with pool_manager.acquire_master() as _conn:
        assert pool_manager._pool_state.get_pool_freesize(sqlalchemy_pool) == 9
    assert pool_manager._pool_state.get_pool_freesize(sqlalchemy_pool) == 10


async def test_is_connection_closed(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert not pool_manager._pool_state.is_connection_closed(conn)
        await conn.close()
        assert pool_manager._pool_state.is_connection_closed(conn)


async def test_metrics(pool_manager):
    async with pool_manager.acquire_master():
        pools = pool_manager.metrics().pools
        assert len(pools) == 1
        p = pools[0]
        assert p.max == 11
        assert p.min == 0
        assert p.used == 2
        assert p.role == "master"
        assert p.healthy is True
        assert p.in_flight == 1
        assert "overflow" in p.extra


@pytest.mark.parametrize("expire_on_commit", [True, False, None])
@pytest.mark.parametrize("autoflush", [True, False, None])
@pytest.mark.parametrize("read_only", [True, False, None])
@pytest.mark.parametrize("class_", [AsyncSession, None])
async def test_async_sessionmaker(
    pool_manager: PoolManager,
    expire_on_commit: bool | None,
    autoflush: bool | None,
    read_only: bool | None,
    class_: type[AsyncSession] | None,
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
