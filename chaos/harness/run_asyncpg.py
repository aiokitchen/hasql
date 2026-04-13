from __future__ import annotations

import asyncio

from hasql.asyncpg import PoolManager

from base import DSN, POOL_MANAGER_KWARGS, harness_main

DRIVER = "asyncpg"


async def execute_write(conn) -> None:
    await conn.execute(
        "INSERT INTO test_data (value) VALUES ($1)",
        "chaos_test",
    )


async def execute_read(conn) -> None:
    await conn.fetchval("SELECT count(*) FROM test_data")


async def main() -> None:
    manager = PoolManager(
        DSN,
        **POOL_MANAGER_KWARGS,
        pool_factory_kwargs={"min_size": 2, "max_size": 5},
    )
    await harness_main(DRIVER, manager, execute_write, execute_read)


if __name__ == "__main__":
    asyncio.run(main())
