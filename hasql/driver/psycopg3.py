from typing import Optional, Sequence

from psycopg import AsyncConnection, errors
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import AsyncConnectionPool

from hasql.abc import PoolDriver
from hasql.metrics import DriverMetrics
from hasql.pool_manager import BasePoolManager
from hasql.utils import Dsn


class Psycopg3AcquireContext:
    __slots__ = ("timeout", "connection", "done", "pool")

    def __init__(
        self,
        pool: AsyncConnectionPool,
        timeout: Optional[float] = None,
    ):
        self.pool = pool
        self.timeout = timeout
        self.connection = None
        self.done = False

    async def __aenter__(self):
        if self.connection is not None or self.done:
            raise errors.InterfaceError("a connection is already acquired")
        self.connection = await self.pool.getconn(self.timeout)
        return self.connection

    async def __aexit__(self, *exc):
        self.done = True
        con = self.connection
        self.connection = None
        await self.pool.putconn(con)

    def __await__(self):
        return self.pool.getconn(self.timeout).__await__()


class Psycopg3Driver(PoolDriver[AsyncConnectionPool, AsyncConnection]):

    def get_pool_freesize(self, pool: AsyncConnectionPool):
        return pool.get_stats()["pool_available"]

    def acquire_from_pool(
        self, pool: AsyncConnectionPool,
        *, timeout=None, **kwargs,
    ):
        return Psycopg3AcquireContext(pool, timeout=timeout, **kwargs)

    async def release_to_pool(
        self,
        connection: AsyncConnection,
        pool: AsyncConnectionPool,
        **kwargs,
    ):
        return await pool.putconn(connection)

    async def is_master(self, connection: AsyncConnection):
        async with connection.cursor() as cur:
            await cur.execute("SHOW transaction_read_only")
            row = await cur.fetchone()
            if row is None:
                raise RuntimeError(
                    "Expected a row from SHOW transaction_read_only",
                )
            return row[0] == "off"

    async def pool_factory(self, dsn: Dsn, **kwargs) -> AsyncConnectionPool:
        pool = AsyncConnectionPool(str(dsn), **kwargs)
        await pool.wait()
        return pool

    def prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        kwargs["min_size"] = kwargs.get("min_size", 1) + 1
        kwargs["max_size"] = kwargs.get("max_size", 10) + 1
        return kwargs

    async def close_pool(self, pool: AsyncConnectionPool):
        await pool.close()

    async def terminate_pool(self, pool: AsyncConnectionPool):
        pass

    def is_connection_closed(self, connection):
        return connection.closed

    def host(self, pool: AsyncConnectionPool):
        conninfo = pool.conninfo
        if not isinstance(conninfo, str):
            return "unknown"
        return conninfo_to_dict(conninfo)["host"]

    def driver_metrics(
        self, pools: Sequence[Optional[AsyncConnectionPool]],
    ) -> Sequence[DriverMetrics]:
        stats = [
            {
                **p.get_stats(),
                "host": self.host(p),
            }
            for p in pools
            if p
        ]
        return [
            DriverMetrics(
                min=stat["pool_min"],
                max=stat["pool_max"],
                idle=stat["pool_available"],
                used=stat["pool_size"],
                host=stat["host"],
            ) for stat in stats
        ]


class PoolManager(BasePoolManager[AsyncConnectionPool, AsyncConnection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=Psycopg3Driver(), **kwargs)


__all__ = ("Psycopg3Driver", "Psycopg3AcquireContext", "PoolManager")
