from __future__ import annotations

import asyncio

from hasql.aiopg import PoolManager

from base import DSN, POOL_MANAGER_KWARGS, harness_main

DRIVER = "aiopg"


async def execute_write(conn) -> None:
    async with conn.cursor() as cur:
        async with cur.begin():
            await cur.execute(
                "INSERT INTO test_data (value) VALUES (%s)",
                ("chaos_test",),
            )


async def execute_read(conn) -> None:
    async with conn.cursor() as cur:
        await cur.execute("SELECT count(*) FROM test_data")
        await cur.fetchone()


async def main() -> None:
    manager = PoolManager(
        DSN,
        **POOL_MANAGER_KWARGS,
        pool_factory_kwargs={"minsize": 2, "maxsize": 5},
    )
    await harness_main(DRIVER, manager, execute_write, execute_read)


if __name__ == "__main__":
    asyncio.run(main())
