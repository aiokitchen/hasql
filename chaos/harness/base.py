from __future__ import annotations

import asyncio
import signal
import time
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone

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


async def _do_operation(
    driver: str,
    manager: BasePoolManager,
    execute_fn: Callable[..., Coroutine],
    acquire_method: str,
    label: str,
) -> None:
    t0 = time.monotonic()
    try:
        async with getattr(manager, acquire_method)() as conn:
            await execute_fn(conn)
            elapsed = int((time.monotonic() - t0) * 1000)
            log(driver, label, f"ok | {elapsed}ms")
    except Exception as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        log(driver, label, f"error | {elapsed}ms | {type(exc).__name__}: {exc}")


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
            await _do_operation(driver, manager, execute_write, "acquire_master", "write")
            await _do_operation(driver, manager, execute_read, "acquire_replica", "read")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log(driver, "stop", "cancelled")
    finally:
        metrics_task.cancel()
        try:
            await metrics_task
        except asyncio.CancelledError:
            pass
        try:
            await manager.close()
        except Exception as exc:
            log(driver, "stop", f"close error: {exc}")
        log(driver, "stop", "closed")


async def harness_main(
    driver: str,
    manager: BasePoolManager,
    execute_write: Callable[..., Coroutine],
    execute_read: Callable[..., Coroutine],
) -> None:
    """Shared entry point for all harness runners."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, stop.set)
    loop.add_signal_handler(signal.SIGTERM, stop.set)

    task = asyncio.create_task(
        run_harness(driver, manager, execute_write, execute_read)
    )

    def _on_task_done(t: asyncio.Task) -> None:
        if not t.cancelled() and t.exception() is not None:
            log(driver, "error", f"harness failed: {t.exception()}")
            stop.set()

    task.add_done_callback(_on_task_done)

    await stop.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
