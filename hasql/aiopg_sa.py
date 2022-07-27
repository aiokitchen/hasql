import aiopg.sa

from hasql.aiopg import PoolManager as AioPgPoolManager
from hasql.utils import Dsn


class PoolManager(AioPgPoolManager):
    async def _is_master(self, connection):
        read_only = await connection.scalar("SHOW transaction_read_only")
        return read_only == "off"

    async def _pool_factory(self, dsn: Dsn):
        return await aiopg.sa.create_engine(
            str(dsn),
            **self.pool_factory_kwargs,
        )


__all__ = ("PoolManager",)
