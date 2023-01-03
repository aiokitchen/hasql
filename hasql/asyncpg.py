import asyncio
from typing import Sequence, Iterable

import asyncpg  # type: ignore

from hasql.base import BasePoolManager
from hasql.metrics import Metrics
from hasql.utils import Dsn


class PoolManager(BasePoolManager):
    pools: Iterable[asyncpg.Pool]

    def get_pool_freesize(self, pool):
        return pool._queue.qsize()

    def acquire_from_pool(self, pool, **kwargs):
        return pool.acquire(**kwargs)

    async def release_to_pool(self, connection, pool, **kwargs):
        await pool.release(connection, **kwargs)

    async def _is_master(self, connection):
        read_only = await connection.fetchrow("SHOW transaction_read_only")
        return read_only[0] == "off"

    async def _pool_factory(self, dsn: Dsn):
        return await asyncpg.create_pool(str(dsn), **self.pool_factory_kwargs)

    def _prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["min_size"] = kwargs.get("min_size", 1) + 1
        kwargs["max_size"] = kwargs.get("max_size", 10) + 1
        return kwargs

    async def _close(self, pool):
        await pool.close()

    async def _terminate(self, pool):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, pool.terminate)

    def is_connection_closed(self, connection):
        return connection.is_closed()

    def _parse_host(self, pool: asyncpg.Pool):
        if len(pool._connect_args) != 1:
            return ""
        return Dsn.parse(pool._connect_args[0]).netloc

    def metrics(self) -> Sequence[Metrics]:
        return [
            Metrics(
                max=p.get_max_size(),
                min=p.get_min_size(),
                idle=p.get_idle_size(),
                used=p.get_size(),
                host=self._parse_host(p),
            ) for p in self.pools
        ]


__all__ = ("PoolManager",)
