from typing import Sequence

import aiopg.sa
from psycopg2._psycopg import parse_dsn

from hasql.aiopg import PoolManager as AioPgPoolManager
from hasql.metrics import Metrics
from hasql.utils import Dsn


class PoolManager(AioPgPoolManager):
    pools: Sequence[aiopg.sa.Engine]  # type: ignore[assignment]

    async def _is_master(self, connection):
        read_only = await connection.scalar("SHOW transaction_read_only")
        return read_only == "off"

    async def _pool_factory(self, dsn: Dsn) -> aiopg.sa.Engine:
        return await aiopg.sa.create_engine(
            str(dsn),
            **self.pool_factory_kwargs,
        )

    def metrics(self) -> Sequence[Metrics]:
        return [
            Metrics(
                max=p.maxsize,
                min=p.minsize,
                idle=p.freesize,
                used=p.size - p.freesize,
                host=parse_dsn(p.dsn).get("host", ""),
            ) for p in self.pools
        ]


__all__ = ("PoolManager",)
