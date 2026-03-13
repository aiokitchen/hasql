import asyncio
from typing import Optional, Sequence

import aiopg

from hasql.abc import PoolDriver
from hasql.acquire import TimeoutAcquireContext
from hasql.metrics import DriverMetrics
from hasql.pool_manager import BasePoolManager
from hasql.utils import Dsn


class AiopgDriver(PoolDriver[aiopg.Pool, aiopg.Connection]):

    def get_pool_freesize(self, pool):
        return pool.freesize

    def acquire_from_pool(self, pool, *, timeout=None, **kwargs):
        ctx = pool.acquire(**kwargs)
        if timeout is not None:
            return TimeoutAcquireContext(ctx, timeout)
        return ctx

    async def release_to_pool(self, connection, pool, **kwargs):
        return await pool.release(connection, **kwargs)

    async def is_master(self, connection):
        cursor = await connection.cursor()
        async with cursor:
            await cursor.execute("SHOW transaction_read_only")
            read_only = await cursor.fetchone()
            return read_only[0] == "off"

    async def pool_factory(self, dsn: Dsn, **kwargs):
        return await aiopg.create_pool(str(dsn), **kwargs)

    def prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["minsize"] = kwargs.get("minsize", 1) + 1
        kwargs["maxsize"] = kwargs.get("maxsize", 10) + 1
        return kwargs

    async def close_pool(self, pool):
        pool.close()
        await pool.wait_closed()

    async def terminate_pool(self, pool):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, pool.terminate)

    def is_connection_closed(self, connection):
        return connection.closed

    def host(self, pool: aiopg.Pool):
        return Dsn.parse(str(pool._dsn)).netloc

    def driver_metrics(
        self, pools: Sequence[Optional[aiopg.Pool]],
    ) -> Sequence[DriverMetrics]:
        return [
            DriverMetrics(
                max=p.maxsize or 0,
                min=p.minsize,
                idle=p.freesize,
                used=p.size - p.freesize,
                host=Dsn.parse(str(p._dsn)).netloc,
            )
            for p in pools
            if p
        ]


class PoolManager(BasePoolManager[aiopg.Pool, aiopg.Connection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=AiopgDriver(), **kwargs)


__all__ = ("AiopgDriver", "PoolManager")
