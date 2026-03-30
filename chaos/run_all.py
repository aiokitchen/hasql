"""Run all drivers against all scenarios and collect results."""
from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import subprocess
import sys
import time

VENV_PYTHON = os.path.join(os.path.dirname(__file__), ".venv", "bin", "python")
HARNESS_DIR = os.path.join(os.path.dirname(__file__), "harness")
SCENARIO_DIR = os.path.join(os.path.dirname(__file__), "scenarios")
CONTROLLER_URL = "http://localhost:8080"

DRIVERS = [
    "asyncpg",
    "aiopg",
    "psycopg3",
    "aiopg_sa",
    "asyncsqlalchemy",
]

SCENARIOS = [
    "freeze_master",
    "replica_failover",
]


def reset_cluster() -> None:
    """Reset cluster to clean state and wait for healthy."""
    # First do a full down/up to handle split-brain
    compose_file = os.path.join(os.path.dirname(__file__), "docker-compose.yml")
    subprocess.run(
        ["sudo", "docker", "compose", "-f", compose_file, "down", "-v"],
        capture_output=True, timeout=30,
    )
    subprocess.run(
        ["sudo", "docker", "compose", "-f", compose_file, "up", "-d"],
        capture_output=True, timeout=60,
    )
    # Wait for all nodes healthy
    for _ in range(30):
        time.sleep(2)
        try:
            import urllib.request
            resp = urllib.request.urlopen(f"{CONTROLLER_URL}/status", timeout=5)
            data = json.loads(resp.read())
            roles = [v.get("role") for v in data.values() if v.get("running")]
            if roles.count("master") == 1 and roles.count("replica") == 2:
                print(f"  Cluster ready: {data}")
                return
        except Exception:
            pass
    raise TimeoutError("Cluster did not become healthy in 60s")


def parse_harness_log(log: str) -> dict:
    """Parse harness log into structured metrics."""
    lines = log.strip().split("\n")
    writes_ok = 0
    writes_err = 0
    reads_ok = 0
    reads_err = 0
    max_write_ms = 0
    max_read_ms = 0
    write_latencies = []
    read_latencies = []
    errors = []
    metrics_timeline = []

    for line in lines:
        # Parse write/read lines
        m = re.match(
            r"(\S+) \| (\S+)\s+\| (write|read)\s+\| (ok|error) \| (\d+)ms(.*)",
            line,
        )
        if m:
            ts, driver, op, status, ms_str, rest = m.groups()
            ms = int(ms_str)
            if op == "write":
                if status == "ok":
                    writes_ok += 1
                    write_latencies.append(ms)
                    max_write_ms = max(max_write_ms, ms)
                else:
                    writes_err += 1
                    errors.append(line.strip())
            else:
                if status == "ok":
                    reads_ok += 1
                    read_latencies.append(ms)
                    max_read_ms = max(max_read_ms, ms)
                else:
                    reads_err += 1
                    errors.append(line.strip())
            continue

        # Parse metrics lines
        m = re.match(r"(\S+) \| \S+\s+\| metrics\s+\| (.*)", line)
        if m:
            ts, detail = m.groups()
            metrics_timeline.append({"ts": ts, "detail": detail})

    return {
        "writes_ok": writes_ok,
        "writes_err": writes_err,
        "reads_ok": reads_ok,
        "reads_err": reads_err,
        "max_write_ms": max_write_ms,
        "max_read_ms": max_read_ms,
        "avg_write_ms": round(sum(write_latencies) / len(write_latencies), 1) if write_latencies else 0,
        "avg_read_ms": round(sum(read_latencies) / len(read_latencies), 1) if read_latencies else 0,
        "p99_write_ms": sorted(write_latencies)[int(len(write_latencies) * 0.99)] if write_latencies else 0,
        "p99_read_ms": sorted(read_latencies)[int(len(read_latencies) * 0.99)] if read_latencies else 0,
        "errors": errors,
        "metrics_timeline": metrics_timeline,
    }


def run_scenario(scenario: str, drivers: list[str], settle_time: float = 10.0) -> dict:
    """Run one scenario with all drivers and return results."""
    print(f"\n{'='*60}")
    print(f"SCENARIO: {scenario}")
    print(f"{'='*60}")

    # Reset cluster
    print("  Resetting cluster...")
    reset_cluster()

    # Start all harnesses
    harness_procs = {}
    harness_logs = {}
    for driver in drivers:
        log_file = f"/tmp/chaos_{scenario}_{driver}.log"
        harness_logs[driver] = log_file
        proc = subprocess.Popen(
            [VENV_PYTHON, f"run_{driver}.py"],
            cwd=HARNESS_DIR,
            stdout=open(log_file, "w"),
            stderr=subprocess.STDOUT,
        )
        harness_procs[driver] = proc
        print(f"  Started {driver} harness (PID {proc.pid})")

    # Wait for harnesses to reach steady state
    print(f"  Waiting {settle_time}s for steady state...")
    time.sleep(settle_time)

    # Check all harnesses still running
    for driver, proc in harness_procs.items():
        if proc.poll() is not None:
            with open(harness_logs[driver]) as f:
                print(f"  WARNING: {driver} exited early! Log:\n{f.read()[-500:]}")

    # Run scenario
    print(f"  Running {scenario}...")
    t0 = time.time()
    scenario_result = subprocess.run(
        [VENV_PYTHON, f"{scenario}.py"],
        cwd=SCENARIO_DIR,
        capture_output=True,
        text=True,
        timeout=180,
    )
    scenario_duration = time.time() - t0
    print(f"  Scenario completed in {scenario_duration:.1f}s")
    print(scenario_result.stdout)
    if scenario_result.returncode != 0:
        print(f"  SCENARIO FAILED: {scenario_result.stderr}")

    # Let harnesses observe post-scenario state for a bit
    print("  Post-scenario observation (5s)...")
    time.sleep(5)

    # Stop all harnesses
    results = {}
    for driver, proc in harness_procs.items():
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

        with open(harness_logs[driver]) as f:
            log_content = f.read()

        parsed = parse_harness_log(log_content)
        parsed["raw_log"] = log_content
        parsed["scenario_stdout"] = scenario_result.stdout
        parsed["scenario_returncode"] = scenario_result.returncode
        results[driver] = parsed
        print(f"  {driver}: {parsed['writes_ok']} writes ok, {parsed['writes_err']} err | "
              f"{parsed['reads_ok']} reads ok, {parsed['reads_err']} err | "
              f"max_write={parsed['max_write_ms']}ms max_read={parsed['max_read_ms']}ms")

    return results


def main() -> None:
    all_results = {}
    for scenario in SCENARIOS:
        all_results[scenario] = run_scenario(scenario, DRIVERS)

    # Save raw results
    with open("/tmp/chaos_results.json", "w") as f:
        # Don't dump raw_log to json, too big
        slim = {}
        for sc, drivers in all_results.items():
            slim[sc] = {}
            for drv, data in drivers.items():
                slim[sc][drv] = {k: v for k, v in data.items() if k not in ("raw_log", "metrics_timeline")}
        json.dump(slim, f, indent=2)

    # Print summary report
    print("\n" + "=" * 80)
    print("CHAOS TEST REPORT")
    print("=" * 80)

    for scenario in SCENARIOS:
        print(f"\n## {scenario}")
        print(f"{'Driver':<20} {'Writes OK':>10} {'Write Err':>10} {'Reads OK':>10} {'Read Err':>10} {'Max W ms':>10} {'Max R ms':>10} {'P99 W ms':>10} {'P99 R ms':>10}")
        print("-" * 110)
        for driver in DRIVERS:
            d = all_results[scenario][driver]
            print(f"{driver:<20} {d['writes_ok']:>10} {d['writes_err']:>10} {d['reads_ok']:>10} {d['reads_err']:>10} {d['max_write_ms']:>10} {d['max_read_ms']:>10} {d['p99_write_ms']:>10} {d['p99_read_ms']:>10}")

        print("\nErrors:")
        for driver in DRIVERS:
            errs = all_results[scenario][driver]["errors"]
            if errs:
                print(f"  {driver}:")
                for e in errs[:5]:
                    print(f"    {e}")
            else:
                print(f"  {driver}: none")


if __name__ == "__main__":
    main()
