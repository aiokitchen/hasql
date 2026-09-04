import asyncio
from typing import Any

import aiopg.sa
from psycopg2.extensions import parse_dsn

from hasql.abc import PoolDriver
from hasql.acquire import AcquireContext, TimeoutAcquireContext
from hasql.metrics import PoolStats
from hasql.pool_manager import BasePoolManager
from hasql.utils import Dsn


class AiopgSaDriver(PoolDriver[aiopg.sa.Engine, aiopg.sa.SAConnection]):

    def get_pool_freesize(self, pool: aiopg.sa.Engine) -> int:
        return pool.freesize

    def acquire_from_pool(
        self,
        pool: aiopg.sa.Engine,
        *,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> AcquireContext[aiopg.sa.SAConnection]:
        context = pool.acquire(**kwargs)
        if timeout is not None:
            return TimeoutAcquireContext(context, timeout)
        return context

    async def release_to_pool(
        self,
        connection: aiopg.sa.SAConnection,
        pool: aiopg.sa.Engine,
        **kwargs: Any,
    ) -> None:
        pool.release(connection, **kwargs)

    async def is_master(self, connection: aiopg.sa.SAConnection) -> bool:
        read_only = await connection.scalar("SHOW transaction_read_only")
        return read_only == "off"

    async def fetch_scalar(
        self,
        connection: aiopg.sa.SAConnection,
        query: str,
    ) -> Any:
        return await connection.scalar(query)

    async def pool_factory(
        self,
        dsn: Dsn,
        **kwargs: Any,
    ) -> aiopg.sa.Engine:
        return await aiopg.sa.create_engine(str(dsn), **kwargs)

    def prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        return {
            **kwargs,
            "minsize": kwargs.get("minsize", 1) + 1,
            "maxsize": kwargs.get("maxsize", 10) + 1,
        }

    async def close_pool(self, pool: aiopg.sa.Engine) -> None:
        pool.close()
        await pool.wait_closed()

    async def terminate_pool(self, pool: aiopg.sa.Engine) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, pool.terminate)

    def is_connection_closed(self, connection: aiopg.sa.SAConnection) -> bool:
        return connection.closed

    def host(self, pool: aiopg.sa.Engine) -> str:
        return parse_dsn(pool.dsn).get("host", "")

    def pool_stats(self, pool: aiopg.sa.Engine) -> PoolStats:
        return PoolStats(
            min=pool.minsize,
            max=pool.maxsize,
            idle=pool.freesize,
            used=pool.size - pool.freesize,
        )


class PoolManager(BasePoolManager[aiopg.sa.Engine, aiopg.sa.SAConnection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=AiopgSaDriver(), **kwargs)


__all__ = ("AiopgSaDriver", "PoolManager")
