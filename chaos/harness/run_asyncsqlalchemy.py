from __future__ import annotations

import asyncio

import sqlalchemy as sa

from hasql.asyncsqlalchemy import PoolManager

from base import DSN, POOL_MANAGER_KWARGS, harness_main

DRIVER = "asyncsqlalchemy"


async def execute_write(conn) -> None:
    await conn.execute(
        sa.text("INSERT INTO test_data (value) VALUES (:val)"),
        {"val": "chaos_test"},
    )
    await conn.commit()


async def execute_read(conn) -> None:
    await conn.scalar(sa.text("SELECT count(*) FROM test_data"))
    await conn.commit()


async def main() -> None:
    manager = PoolManager(
        DSN,
        **POOL_MANAGER_KWARGS,
        pool_factory_kwargs={"pool_size": 5},
    )
    await harness_main(DRIVER, manager, execute_write, execute_read)


if __name__ == "__main__":
    asyncio.run(main())
