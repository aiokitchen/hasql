import asyncpg  # type: ignore[import-untyped]
import asyncpgsa  # type: ignore

from hasql.driver.asyncpg import AsyncpgDriver
from hasql.pool_manager import BasePoolManager
from hasql.utils import Dsn


class AsyncpgsaDriver(AsyncpgDriver):
    async def pool_factory(self, dsn: Dsn, **kwargs):
        return await asyncpgsa.create_pool(str(dsn), **kwargs)


class PoolManager(BasePoolManager[asyncpg.Pool, asyncpg.Connection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=AsyncpgsaDriver(), **kwargs)


__all__ = ("AsyncpgsaDriver", "PoolManager")
