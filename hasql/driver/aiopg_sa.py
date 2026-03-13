from typing import Optional, Sequence

import aiopg.sa
from psycopg2.extensions import parse_dsn

from hasql.driver.aiopg import AiopgDriver
from hasql.metrics import DriverMetrics
from hasql.pool_manager import BasePoolManager
from hasql.utils import Dsn


class AiopgSaDriver(AiopgDriver):

    async def is_master(self, connection):
        read_only = await connection.scalar("SHOW transaction_read_only")
        return read_only == "off"

    async def pool_factory(self, dsn: Dsn, **kwargs):
        return await aiopg.sa.create_engine(str(dsn), **kwargs)

    def host(self, pool) -> str:
        return parse_dsn(pool.dsn).get("host", "")

    def driver_metrics(  # type: ignore[override]
        self, pools: Sequence[Optional[aiopg.sa.Engine]],
    ) -> Sequence[DriverMetrics]:
        return [
            DriverMetrics(
                max=p.maxsize,
                min=p.minsize,
                idle=p.freesize,
                used=p.size - p.freesize,
                host=parse_dsn(p.dsn).get("host", ""),
            )
            for p in pools
            if p
        ]


class PoolManager(BasePoolManager[aiopg.sa.Engine, aiopg.sa.SAConnection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=AiopgSaDriver(), **kwargs)


__all__ = ("AiopgSaDriver", "PoolManager")
