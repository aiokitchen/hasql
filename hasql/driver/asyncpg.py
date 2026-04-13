import asyncio
import re
from typing import ClassVar

import asyncpg  # type: ignore[import-untyped]

from hasql.abc import PoolDriver
from hasql.metrics import PoolStats
from hasql.pool_manager import BasePoolManager
from hasql.utils import Dsn


def _asyncpg_version() -> tuple[int, ...]:
    return tuple(
        int(re.match(r"\d+", part).group())  # type: ignore[union-attr]
        for part in asyncpg.__version__.split(".")[:3]
        if re.match(r"\d+", part)
    )


class AsyncpgDriver(PoolDriver[asyncpg.Pool, asyncpg.Connection]):
    cached_hosts: ClassVar[dict[int, str]] = {}

    def get_pool_freesize(self, pool):
        return pool._queue.qsize()

    def acquire_from_pool(self, pool, *, timeout=None, **kwargs):
        return pool.acquire(timeout=timeout, **kwargs)

    async def release_to_pool(self, connection, pool, **kwargs):
        await pool.release(connection, **kwargs)

    async def is_master(self, connection):
        read_only = await connection.fetchrow("SHOW transaction_read_only")
        return read_only[0] == "off"

    async def fetch_scalar(self, connection, query):
        return await connection.fetchval(query)

    async def pool_factory(self, dsn: Dsn, **kwargs):
        return await asyncpg.create_pool(str(dsn), **kwargs)

    def prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        return {
            **kwargs,
            "min_size": kwargs.get("min_size", 1) + 1,
            "max_size": kwargs.get("max_size", 10) + 1,
        }

    async def close_pool(self, pool):
        await pool.close()

    async def terminate_pool(self, pool):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, pool.terminate)

    def is_connection_closed(self, connection):
        return connection.is_closed()

    if _asyncpg_version() >= (0, 29, 0):
        def host(self, pool: asyncpg.Pool):
            conn = next(
                (holder._con for holder in pool._holders
                 if holder._con),
                None,
            )
            if conn is not None:
                addr, _ = conn._addr
                AsyncpgDriver.cached_hosts[id(pool)] = addr
            return AsyncpgDriver.cached_hosts.get(
                id(pool), "unknown",
            )
    else:
        def host(self, pool: asyncpg.Pool):
            addr, _ = pool._working_addr
            return addr

    def pool_stats(self, pool: asyncpg.Pool) -> PoolStats:
        idle = self.get_pool_freesize(pool)
        return PoolStats(
            min=pool._minsize,
            max=pool._maxsize,
            idle=idle,
            used=pool.get_size() - idle,
        )


class PoolManager(BasePoolManager[asyncpg.Pool, asyncpg.Connection]):
    cached_hosts: ClassVar[dict[int, str]] = (
        AsyncpgDriver.cached_hosts
    )

    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=AsyncpgDriver(), **kwargs)


__all__ = ("AsyncpgDriver", "PoolManager")
