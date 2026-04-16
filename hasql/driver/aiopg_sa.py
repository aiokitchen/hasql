import aiopg.sa
from psycopg2.extensions import parse_dsn

from hasql.driver.aiopg import AiopgDriver
from hasql.metrics import PoolStats
from hasql.pool_manager import BasePoolManager
from hasql.utils import Dsn


class AiopgSaDriver(AiopgDriver):

    async def is_master(self, connection: aiopg.sa.SAConnection) -> bool:  # type: ignore[override]
        read_only = await connection.scalar("SHOW transaction_read_only")
        return read_only == "off"

    async def fetch_scalar(self, connection: aiopg.sa.SAConnection, query: str):  # type: ignore[override]
        return await connection.scalar(query)

    async def pool_factory(self, dsn: Dsn, **kwargs) -> aiopg.sa.Engine:  # type: ignore[override]
        return await aiopg.sa.create_engine(str(dsn), **kwargs)

    def host(self, pool: aiopg.sa.Engine) -> str:  # type: ignore[override]
        return parse_dsn(pool.dsn).get("host", "")

    def pool_stats(self, pool: aiopg.sa.Engine) -> PoolStats:  # type: ignore[override]
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
