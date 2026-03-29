from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from typing import Any

from hasql.base import BasePoolManager


DSN = "postgresql://testuser:testpass@localhost:15432,localhost:15433,localhost:15434/testdb"

POOL_MANAGER_KWARGS = {
    "refresh_delay": 1,
    "refresh_timeout": 5,
    "acquire_timeout": 3,
    "fallback_master": True,
}


def log(driver: str, op: str, detail: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"{ts} | {driver:<16} | {op:<8} | {detail}", flush=True)


async def _log_metrics(driver: str, manager: BasePoolManager) -> None:
    """Periodic metrics dump every 1 second."""
    while True:
        await asyncio.sleep(1)
        try:
            m = manager.metrics()
            pool_details = []
            for d in m.drivers:
                pool_details.append(
                    f"{d.host}(idle={d.idle},used={d.used},max={d.max})"
                )
            detail = (
                f"masters={manager.master_pool_count} "
                f"replicas={manager.replica_pool_count} "
                f"pools=[{', '.join(pool_details)}]"
            )
            log(driver, "metrics", detail)
        except Exception as exc:
            log(driver, "metrics", f"error: {exc}")


async def _do_write(
    driver: str,
    manager: BasePoolManager,
    execute_write: Callable[..., Coroutine],
) -> None:
    t0 = time.monotonic()
    try:
        async with manager.acquire_master() as conn:
            await execute_write(conn)
            elapsed = int((time.monotonic() - t0) * 1000)
            log(driver, "write", f"ok | {elapsed}ms")
    except Exception as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        log(driver, "write", f"error | {elapsed}ms | {type(exc).__name__}: {exc}")


async def _do_read(
    driver: str,
    manager: BasePoolManager,
    execute_read: Callable[..., Coroutine],
) -> None:
    t0 = time.monotonic()
    try:
        async with manager.acquire_replica() as conn:
            await execute_read(conn)
            elapsed = int((time.monotonic() - t0) * 1000)
            log(driver, "read", f"ok | {elapsed}ms")
    except Exception as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        log(driver, "read", f"error | {elapsed}ms | {type(exc).__name__}: {exc}")


async def run_harness(
    driver: str,
    manager: BasePoolManager,
    execute_write: Callable[..., Coroutine],
    execute_read: Callable[..., Coroutine],
    interval: float = 0.5,
) -> None:
    """Main harness loop. Runs until cancelled."""
    log(driver, "init", "waiting for ready(masters=1, replicas=2, timeout=30)")
    await manager.ready(masters_count=1, replicas_count=2, timeout=30)
    log(driver, "init", "cluster ready")

    metrics_task = asyncio.create_task(_log_metrics(driver, manager))
    try:
        while True:
            await _do_write(driver, manager, execute_write)
            await _do_read(driver, manager, execute_read)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log(driver, "stop", "cancelled")
    finally:
        metrics_task.cancel()
        try:
            await metrics_task
        except asyncio.CancelledError:
            pass
        await manager.close()
        log(driver, "stop", "closed")
