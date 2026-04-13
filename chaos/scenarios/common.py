"""Shared utilities for chaos scenarios."""
from __future__ import annotations

import asyncio
import time

import httpx

CONTROLLER = "http://localhost:8080"
GATE_TIMEOUT = 60


async def api(method: str, path: str, client: httpx.AsyncClient | None = None) -> dict:
    if client is None:
        async with httpx.AsyncClient() as c:
            resp = await c.request(method, f"{CONTROLLER}{path}", timeout=30)
            resp.raise_for_status()
            return resp.json()
    resp = await client.request(method, f"{CONTROLLER}{path}", timeout=30)
    resp.raise_for_status()
    return resp.json()


async def wait_gate(
    description: str,
    check_fn,
    timeout: float = GATE_TIMEOUT,
    poll_interval: float = 1.0,
) -> float:
    """Poll check_fn until it returns True. Returns elapsed seconds.

    Raises TimeoutError if the gate does not pass within the timeout.
    """
    print(f"  gate: waiting for {description} (timeout={timeout}s)")
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if await check_fn():
            elapsed = time.monotonic() - t0
            print(f"  gate: {description} — passed in {elapsed:.1f}s")
            return elapsed
        await asyncio.sleep(poll_interval)
    elapsed = time.monotonic() - t0
    raise TimeoutError(f"Gate '{description}' timed out after {elapsed:.1f}s")
