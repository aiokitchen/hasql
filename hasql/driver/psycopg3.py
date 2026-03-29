from psycopg import AsyncConnection, errors
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import AsyncConnectionPool

from hasql.abc import PoolDriver
from hasql.metrics import PoolStats
from hasql.pool_manager import BasePoolManager
from hasql.utils import Dsn


class Psycopg3AcquireContext:
    __slots__ = ("timeout", "connection", "done", "pool")

    def __init__(
        self,
        pool: AsyncConnectionPool,
        timeout: float | None = None,
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
        if con is not None:
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
        return Psycopg3AcquireContext(pool, timeout=timeout)

    async def release_to_pool(
        self,
        connection: AsyncConnection,
        pool: AsyncConnectionPool,
        **kwargs,
    ):
        await pool.putconn(connection)

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
        pool = AsyncConnectionPool(str(dsn), open=False, **kwargs)
        await pool.open()
        await pool.wait()
        return pool

    def prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        return {
            **kwargs,
            "min_size": kwargs.get("min_size", 1) + 1,
            "max_size": kwargs.get("max_size", 10) + 1,
        }

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

    def pool_stats(self, pool: AsyncConnectionPool) -> PoolStats:
        stats = pool.get_stats()
        return PoolStats(
            min=stats["pool_min"],
            max=stats["pool_max"],
            idle=stats["pool_available"],
            used=stats["pool_size"] - stats["pool_available"],
            extra={
                "pool_size": stats["pool_size"],
                "requests_waiting": stats.get("requests_waiting", 0),
                "requests_num": stats.get("requests_num", 0),
                "requests_queued": stats.get("requests_queued", 0),
                "requests_wait_ms": stats.get("requests_wait_ms", 0),
                "requests_errors": stats.get("requests_errors", 0),
                "returns_bad": stats.get("returns_bad", 0),
                "connections_num": stats.get("connections_num", 0),
                "connections_ms": stats.get("connections_ms", 0),
                "connections_errors": stats.get("connections_errors", 0),
                "connections_lost": stats.get("connections_lost", 0),
                "usage_ms": stats.get("usage_ms", 0),
            },
        )


class PoolManager(BasePoolManager[AsyncConnectionPool, AsyncConnection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=Psycopg3Driver(), **kwargs)


__all__ = ("Psycopg3Driver", "Psycopg3AcquireContext", "PoolManager")
