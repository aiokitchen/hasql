import asyncio
from contextlib import _AsyncGeneratorContextManager, asynccontextmanager
from typing import Any, AsyncIterator, Callable, Dict, Optional, Type

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import QueuePool

from hasql.abc import PoolDriver
from hasql.acquire import TimeoutAcquireContext
from hasql.metrics import PoolStats
from hasql.pool_manager import BasePoolManager
from hasql.utils import Dsn


class AsyncSqlAlchemyDriver(PoolDriver[AsyncEngine, AsyncConnection]):

    def get_pool_freesize(self, pool: AsyncEngine):
        queue_pool: QueuePool = pool.sync_engine.pool
        return queue_pool.size() - queue_pool.checkedout()

    def acquire_from_pool(
        self, pool: AsyncEngine,
        *, timeout=None, **kwargs,
    ):
        ctx = pool.connect()
        if timeout is not None:
            return TimeoutAcquireContext(ctx, timeout)
        return ctx

    async def release_to_pool(
        self,
        connection: AsyncConnection,
        pool: AsyncEngine,
        **kwargs,
    ):
        await connection.close()

    async def is_master(self, connection: AsyncConnection):
        result = await connection.scalar(
            sa.text("SHOW transaction_read_only"),
        ) == "off"
        await connection.execute(sa.text('COMMIT'))
        return result

    async def pool_factory(self, dsn: Dsn, **kwargs):
        d = str(dsn)
        if d.startswith('postgresql://'):
            d = d.replace('postgresql://', 'postgresql+asyncpg://', 1)
        elif d.startswith('postgres://'):
            d = d.replace('postgres://', 'postgresql+asyncpg://', 1)
        return create_async_engine(d, **kwargs)

    def prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["pool_size"] = kwargs.get("pool_size", 1) + 1
        return kwargs

    async def close_pool(self, pool: AsyncEngine):
        await pool.dispose()

    async def terminate_pool(self, pool: AsyncEngine):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, pool.sync_engine.dispose)

    def is_connection_closed(self, connection: AsyncConnection):
        return connection.closed

    def host(self, pool: AsyncEngine):
        return pool.sync_engine.url.host or "unknown"

    def pool_stats(self, pool: AsyncEngine) -> PoolStats:
        qp = pool.sync_engine.pool
        return PoolStats(
            min=0,
            max=qp.size(),
            idle=qp.checkedin(),
            used=qp.checkedout(),
            extra={
                "overflow": qp.overflow(),
            },
        )


class PoolManager(BasePoolManager[AsyncEngine, AsyncConnection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=AsyncSqlAlchemyDriver(), **kwargs)


def async_sessionmaker(
    pool_manager: PoolManager,
    *,
    class_: Type[AsyncSession] = AsyncSession,
    autoflush: bool = True,
    expire_on_commit: bool = True,
    info: Optional[Dict[Any, Any]] = None,
    acquire_kwargs: Optional[Dict[str, Any]] = None,
    **kw: Any,
) -> Callable[..., _AsyncGeneratorContextManager]:
    """Create async session maker with hasql pool support.

    This function replaces the default `async_sessionmaker` from
    SQLAlchemy to work with the `PoolManager` class. It allows you to
    create an async session that is bound to a connection acquired from
    the pool. The session will automatically release the connection
    back to the pool when the session is closed.

    You also can specify the session class to use with the `class_`
    parameter, and you can customize the session's behavior with
    parameters like `autoflush`, `expire_on_commit`, and `info`.

    Use the `acquire_kwargs` to pass additional parameters to the
    `pool.acquire()` method. E.g. to create a session with replica
    connection:
    >>> ReplicaSession = async_sessionmaker(
    >>>     pool_manager,
    >>>     acquire_kwargs={"read_only": True}
    >>> )

    """
    if acquire_kwargs is None:
        acquire_kwargs = {}

    @asynccontextmanager
    async def create_async_session() -> AsyncIterator[AsyncSession]:
        """Create an async session with connection from the pool."""
        async with (
            pool_manager.acquire(**acquire_kwargs) as connection,
            class_(
                bind=connection,
                autoflush=autoflush,
                expire_on_commit=expire_on_commit,
                info=info,
                **kw,
            ) as session,
        ):
            yield session

    return create_async_session


__all__ = ("AsyncSqlAlchemyDriver", "PoolManager", "async_sessionmaker")
