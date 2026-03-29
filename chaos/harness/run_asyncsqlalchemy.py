from __future__ import annotations

import asyncio
import signal

import sqlalchemy as sa

from hasql.asyncsqlalchemy import PoolManager

from base import DSN, POOL_MANAGER_KWARGS, log, run_harness

DRIVER = "asyncsqlalchemy"


async def execute_write(conn) -> None:
    await conn.execute(
        sa.text("INSERT INTO test_data (value) VALUES (:val)"),
        {"val": "chaos_test"},
    )
    await conn.execute(sa.text("COMMIT"))


async def execute_read(conn) -> None:
    await conn.scalar(sa.text("SELECT count(*) FROM test_data"))
    await conn.execute(sa.text("COMMIT"))


async def main() -> None:
    manager = PoolManager(
        DSN,
        **POOL_MANAGER_KWARGS,
        pool_factory_kwargs={"pool_size": 5},
    )

    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, stop.set)
    loop.add_signal_handler(signal.SIGTERM, stop.set)

    task = asyncio.create_task(
        run_harness(DRIVER, manager, execute_write, execute_read)
    )

    await stop.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
