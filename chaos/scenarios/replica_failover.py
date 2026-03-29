"""Replica failover scenario.

Phases:
  1. Verify steady state (1 master, 2 replicas)
  2. Kill pg-replica-1 -> replicas=1
  3. Kill pg-replica-2 -> replicas=0, reads fall back to master
  4. Restart pg-replica-1 -> replicas=1
  5. Restart pg-replica-2 -> replicas=2
  6. Print summary
"""
from __future__ import annotations

import asyncio
import time

from common import api, wait_gate


async def count_replicas() -> int:
    try:
        status = await api("GET", "/status")
        return sum(1 for v in status.values() if v.get("role") == "replica")
    except Exception:
        return -1


async def _replicas_gte(n: int) -> bool:
    return (await count_replicas()) >= n


async def _replicas_eq(n: int) -> bool:
    return (await count_replicas()) == n


async def run() -> None:
    killed: list[str] = []
    timestamps: dict[str, float] = {}
    t_start = time.monotonic()

    try:
        # Phase 1: Steady state
        print("\n=== Phase 1: Verify steady state ===")
        await wait_gate("2 replicas available", lambda: _replicas_gte(2))

        # Phase 2: Kill first replica
        print("\n=== Phase 2: Kill pg-replica-1 ===")
        await api("POST", "/kill/pg-replica-1")
        killed.append("pg-replica-1")
        timestamps["kill_1"] = time.monotonic()
        print(f"  POST /kill/pg-replica-1 at T+{timestamps['kill_1'] - t_start:.1f}s")

        await wait_gate("replicas=1", lambda: _replicas_eq(1))
        timestamps["kill_1_detected"] = time.monotonic()

        # Phase 3: Kill second replica
        print("\n=== Phase 3: Kill pg-replica-2 ===")
        await api("POST", "/kill/pg-replica-2")
        killed.append("pg-replica-2")
        timestamps["kill_2"] = time.monotonic()
        print(f"  POST /kill/pg-replica-2 at T+{timestamps['kill_2'] - t_start:.1f}s")

        await wait_gate("replicas=0", lambda: _replicas_eq(0))
        timestamps["kill_2_detected"] = time.monotonic()
        print("  reads should now fall back to master (fallback_master=True)")

        # Phase 4: Restart first replica
        print("\n=== Phase 4: Restart pg-replica-1 ===")
        await api("POST", "/restart/pg-replica-1")
        killed.remove("pg-replica-1")
        timestamps["restart_1"] = time.monotonic()
        print(f"  POST /restart/pg-replica-1 at T+{timestamps['restart_1'] - t_start:.1f}s")

        await wait_gate("replicas>=1", lambda: _replicas_gte(1))
        timestamps["restart_1_detected"] = time.monotonic()

        # Phase 5: Restart second replica
        print("\n=== Phase 5: Restart pg-replica-2 ===")
        await api("POST", "/restart/pg-replica-2")
        killed.remove("pg-replica-2")
        timestamps["restart_2"] = time.monotonic()
        print(f"  POST /restart/pg-replica-2 at T+{timestamps['restart_2'] - t_start:.1f}s")

        await wait_gate("replicas=2", lambda: _replicas_gte(2))
        timestamps["restart_2_detected"] = time.monotonic()

        # Phase 6: Summary
        print("\n=== Summary ===")
        print(f"  Total duration: {timestamps['restart_2_detected'] - t_start:.1f}s")
        print(f"  Kill replica-1 -> detected: {timestamps['kill_1_detected'] - timestamps['kill_1']:.1f}s")
        print(f"  Kill replica-2 -> detected: {timestamps['kill_2_detected'] - timestamps['kill_2']:.1f}s")
        print(f"  Restart replica-1 -> detected: {timestamps['restart_1_detected'] - timestamps['restart_1']:.1f}s")
        print(f"  Restart replica-2 -> detected: {timestamps['restart_2_detected'] - timestamps['restart_2']:.1f}s")
        print()
        final_status = await api("GET", "/status")
        for node, info in final_status.items():
            print(f"  {node}: {info}")

    finally:
        print("\n=== Cleanup ===")
        for node in killed:
            print(f"  restarting {node}")
            try:
                await api("POST", f"/restart/{node}")
            except Exception as exc:
                print(f"  warning: restart {node} failed: {exc}")


if __name__ == "__main__":
    asyncio.run(run())
