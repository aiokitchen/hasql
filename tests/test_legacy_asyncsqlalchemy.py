import mock

from hasql.asyncsqlalchemy import PoolManager
from hasql.utils import Dsn


async def test_pool_factory_converts_postgres_scheme_to_asyncpg():
    pool_manager = PoolManager.__new__(PoolManager)
    pool_manager._pool_factory_kwargs = {}
    engine = object()
    dsn = Dsn(
        scheme="postgres",
        netloc="localhost:5432",
        user="user",
        password="password",
        dbname="database",
    )

    with mock.patch(
        "hasql.asyncsqlalchemy.create_async_engine",
        return_value=engine,
    ) as create_async_engine:
        result = await pool_manager._pool_factory(dsn)

    assert (result, create_async_engine.call_args) == (
        engine,
        mock.call(
            "postgresql+asyncpg://user:password@localhost:5432/database",
        ),
    )
