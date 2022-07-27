import sqlalchemy as sa
from sqlalchemy.ext.asyncio import (
    create_async_engine, AsyncEngine, AsyncConnection
)
from sqlalchemy.pool import QueuePool

from hasql.base import BasePoolManager
from hasql.utils import Dsn


class PoolManager(BasePoolManager):
    def get_pool_freesize(self, pool: AsyncEngine):
        queue_pool: QueuePool = pool.sync_engine.pool
        return queue_pool.size() - queue_pool.checkedout()

    def acquire_from_pool(self, pool: AsyncEngine, **kwargs):
        return pool.connect()

    async def release_to_pool(
        self,
        connection: AsyncConnection,
        _: AsyncEngine,
        **kwargs
    ):
        await connection.close()

    async def _is_master(self, connection: AsyncConnection):
        return await connection.scalar(
            sa.text("SHOW transaction_read_only")
        ) == "off"

    async def _pool_factory(self, dsn: Dsn):
        # TODO: Add support of psycopg3 after release of sqlalchemy 2.0
        d = str(dsn).replace("postgresql", "postgresql+asyncpg")
        return create_async_engine(d, **self.pool_factory_kwargs)

    def _prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["pool_size"] = kwargs.get("pool_size", 1) + 1
        return kwargs

    async def _close(self, pool: AsyncEngine):
        await pool.dispose()

    def _terminate(self, pool: AsyncEngine):
        # pool don't have terminate, use sync dispose instead
        pool.sync_engine.dispose()

    def is_connection_closed(self, connection: AsyncConnection):
        return connection.closed


__all__ = ["PoolManager"]
