"""Master freeze + promote scenario.

Phases:
  1. Start all 5 driver harnesses, wait for steady state
  2. Freeze master (iptables DROP)
  3. Wait until master no longer reachable via /status
  4. Promote pg-replica-1 to master
  5. Wait until pg-replica-1 detected as master
  6. Unfreeze old master, observe for 15s
  7. Print per-driver summary
"""
from __future__ import annotations

import asyncio
import time

from common import api, wait_gate


async def check_status_masters(expected: int) -> bool:
    try:
        status = await api("GET", "/status")
        masters = sum(1 for v in status.values() if v.get("role") == "master")
        return masters >= expected
    except Exception:
        return False


async def check_status_no_master_on(node: str) -> bool:
    try:
        status = await api("GET", "/status")
        return status.get(node, {}).get("role") != "master"
    except Exception:
        return False


async def check_node_is_master(node: str) -> bool:
    try:
        status = await api("GET", "/status")
        return status.get(node, {}).get("role") == "master"
    except Exception:
        return False


async def run() -> None:
    frozen: list[str] = []
    timestamps: dict[str, float] = {}

    try:
        # Phase 1: Verify steady state
        print("\n=== Phase 1: Verify steady state ===")
        print("  (Ensure harnesses are running separately)")
        t_start = time.monotonic()

        await wait_gate(
            "cluster has 1 master",
            lambda: check_status_masters(1),
        )
        timestamps["start"] = time.monotonic()

        # Phase 2: Freeze master
        print("\n=== Phase 2: Freeze master ===")
        await api("POST", "/freeze/pg-master")
        frozen.append("pg-master")
        timestamps["freeze"] = time.monotonic()
        print(f"  POST /freeze/pg-master at T+{timestamps['freeze'] - t_start:.1f}s")

        await wait_gate(
            "master no longer reachable via /status",
            lambda: check_status_no_master_on("pg-master"),
        )
        timestamps["freeze_detected"] = time.monotonic()

        # Phase 3: Promote replica
        print("\n=== Phase 3: Promote pg-replica-1 ===")
        await api("POST", "/promote/pg-replica-1")
        timestamps["promote"] = time.monotonic()
        print(f"  POST /promote/pg-replica-1 at T+{timestamps['promote'] - t_start:.1f}s")

        await wait_gate(
            "pg-replica-1 is master",
            lambda: check_node_is_master("pg-replica-1"),
        )
        timestamps["promote_detected"] = time.monotonic()

        # Phase 4: Unfreeze old master
        print("\n=== Phase 4: Unfreeze old master ===")
        await api("POST", "/unfreeze/pg-master")
        frozen.remove("pg-master")
        timestamps["unfreeze"] = time.monotonic()
        print(f"  POST /unfreeze/pg-master at T+{timestamps['unfreeze'] - t_start:.1f}s")
        print("  observing for 15s...")
        await asyncio.sleep(15)
        timestamps["end"] = time.monotonic()

        # Phase 5: Summary
        print("\n=== Summary ===")
        print(f"  Total duration: {timestamps['end'] - t_start:.1f}s")
        print(f"  Freeze -> detection: {timestamps['freeze_detected'] - timestamps['freeze']:.1f}s")
        print(f"  Promote -> detection: {timestamps['promote_detected'] - timestamps['promote']:.1f}s")
        print()
        final_status = await api("GET", "/status")
        for node, info in final_status.items():
            print(f"  {node}: {info}")

    finally:
        print("\n=== Cleanup ===")
        for node in frozen:
            print(f"  unfreezing {node}")
            try:
                await api("POST", f"/unfreeze/{node}")
            except Exception as exc:
                print(f"  warning: unfreeze {node} failed: {exc}")


if __name__ == "__main__":
    asyncio.run(run())
