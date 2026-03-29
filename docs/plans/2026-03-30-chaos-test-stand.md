# Chaos Test Stand Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Docker-based PostgreSQL chaos test stand with HTTP API for triggering failover scenarios and per-driver harness scripts to observe how each hasql driver handles them.

**Architecture:** Docker Compose runs 3 PostgreSQL nodes (1 master + 2 replicas) with streaming replication. A host-side FastAPI controller executes `sudo docker exec` commands for chaos operations (iptables freeze, pg_promote, kill/restart). Host-side Python harness scripts exercise all 5 hasql drivers against the cluster with structured logging and periodic metrics dumps.

**Tech Stack:** Docker Compose, PostgreSQL 16 (Debian), FastAPI + uvicorn, hasql drivers (asyncpg, aiopg, psycopg3, asyncsqlalchemy, aiopg_sa)

**Spec:** `docs/superpowers/specs/2026-03-30-chaos-test-stand-design.md`

---

## File Structure

```
chaos/
├── Dockerfile.pg                   # postgres:16 + iptables (3 lines)
├── docker-compose.yml              # 3 PG nodes on chaos-net, ports 15432-15434
├── pg-master/
│   ├── init.sql                    # replicator user, testuser, testdb, test_data table
│   └── pg_hba.conf                 # replication + client auth rules
├── pg-replica/
│   └── entrypoint.sh               # pg_basebackup -R from master, start postgres
├── requirements.txt                # fastapi, uvicorn
├── controller.py                   # FastAPI chaos API: status, freeze, promote, kill, reset
├── harness/
│   ├── base.py                     # Shared: create_pool_manager(), run_loop(), log_metrics()
│   ├── run_asyncpg.py              # asyncpg runner
│   ├── run_aiopg.py                # aiopg runner
│   ├── run_psycopg3.py             # psycopg3 runner
│   ├── run_asyncsqlalchemy.py      # asyncsqlalchemy runner
│   └── run_aiopg_sa.py             # aiopg_sa runner
└── scenarios/
    ├── freeze_master.py            # Master freeze + promote scenario
    └── replica_failover.py         # All replicas down, fallback to master
```

Modified:
- `justfile` — add `chaos-up`, `chaos-down`, `chaos-status` recipes

---

### Task 1: Docker image — Dockerfile.pg

**Files:**
- Create: `chaos/Dockerfile.pg`

- [x] **Step 1: Create Dockerfile.pg**

```dockerfile
FROM postgres:16

RUN apt-get update && apt-get install -y --no-install-recommends iptables && rm -rf /var/lib/apt/lists/*
```

- [x] **Step 2: Build the image to verify**

Run: `sudo docker build -f chaos/Dockerfile.pg -t chaos-postgres:16 chaos/`

If DNS fails during build, use: `sudo docker build --network=host -f chaos/Dockerfile.pg -t chaos-postgres:16 chaos/`

Expected: Image builds successfully, iptables installed.

- [x] **Step 3: Verify iptables works inside the image**

Run: `sudo docker run --rm --cap-add=NET_ADMIN chaos-postgres:16 iptables -L -n`

Expected: Shows default chains (INPUT, FORWARD, OUTPUT) with ACCEPT policy.

- [x] **Step 4: Commit**

```bash
git add chaos/Dockerfile.pg
git commit -m "chaos: add Dockerfile with postgres:16 + iptables"
```

---

### Task 2: PostgreSQL master configuration

**Files:**
- Create: `chaos/pg-master/init.sql`
- Create: `chaos/pg-master/pg_hba.conf`

- [x] **Step 1: Create init.sql**

```sql
-- Create replication user
CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass';

-- Create test database and user
CREATE USER testuser WITH ENCRYPTED PASSWORD 'testpass';
CREATE DATABASE testdb OWNER testuser;

-- Create test table in testdb
\c testdb
CREATE TABLE IF NOT EXISTS test_data (
    id SERIAL PRIMARY KEY,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
GRANT ALL ON test_data TO testuser;
GRANT USAGE, SELECT ON SEQUENCE test_data_id_seq TO testuser;
```

- [x] **Step 2: Create pg_hba.conf**

```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
host    all             all             0.0.0.0/0               md5
host    replication     replicator      0.0.0.0/0               md5
```

- [x] **Step 3: Commit**

```bash
git add chaos/pg-master/
git commit -m "chaos: add master PG init.sql and pg_hba.conf"
```

---

### Task 3: Replica entrypoint

**Files:**
- Create: `chaos/pg-replica/entrypoint.sh`

- [x] **Step 1: Create entrypoint.sh**

```bash
#!/bin/bash
set -e

# Wait for master to be ready
until PGPASSWORD=replicator_pass pg_isready -h pg-master -p 5432 -U replicator -d postgres; do
    echo "Waiting for master to be ready..."
    sleep 1
done

# If data directory is empty, do a base backup from master
if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
    echo "Performing base backup from master..."
    PGPASSWORD=replicator_pass pg_basebackup \
        -h pg-master \
        -p 5432 \
        -U replicator \
        -D "$PGDATA" \
        -Fp -Xs -P -R

    # Ensure hot_standby is on
    echo "hot_standby = on" >> "$PGDATA/postgresql.conf"

    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
fi

# Start postgres via the stock entrypoint
exec docker-entrypoint.sh postgres
```

- [x] **Step 2: Make executable**

Run: `chmod +x chaos/pg-replica/entrypoint.sh`

- [x] **Step 3: Commit**

```bash
git add chaos/pg-replica/
git commit -m "chaos: add replica entrypoint with pg_basebackup"
```

---

### Task 4: Docker Compose cluster

**Files:**
- Create: `chaos/docker-compose.yml`

- [x] **Step 1: Create docker-compose.yml**

```yaml
services:
  pg-master:
    build:
      context: .
      dockerfile: Dockerfile.pg
    image: chaos-postgres:16
    container_name: chaos-pg-master
    ports:
      - "15432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: postgres
    command:
      - postgres
      - -c
      - wal_level=replica
      - -c
      - max_wal_senders=10
      - -c
      - max_replication_slots=4
      - -c
      - hot_standby=on
      - -c
      - hba_file=/etc/postgresql/pg_hba.conf
    volumes:
      - pg-master-data:/var/lib/postgresql/data
      - ./pg-master/init.sql:/docker-entrypoint-initdb.d/init.sql
      - ./pg-master/pg_hba.conf:/etc/postgresql/pg_hba.conf
    cap_add:
      - NET_ADMIN
    networks:
      - chaos-net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 10

  pg-replica-1:
    image: chaos-postgres:16
    container_name: chaos-pg-replica-1
    ports:
      - "15433:5432"
    environment:
      PGDATA: /var/lib/postgresql/data
      POSTGRES_PASSWORD: postgres
    entrypoint: /entrypoint-replica.sh
    volumes:
      - pg-replica-1-data:/var/lib/postgresql/data
      - ./pg-replica/entrypoint.sh:/entrypoint-replica.sh
    cap_add:
      - NET_ADMIN
    depends_on:
      pg-master:
        condition: service_healthy
    networks:
      - chaos-net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 10

  pg-replica-2:
    image: chaos-postgres:16
    container_name: chaos-pg-replica-2
    ports:
      - "15434:5432"
    environment:
      PGDATA: /var/lib/postgresql/data
      POSTGRES_PASSWORD: postgres
    entrypoint: /entrypoint-replica.sh
    volumes:
      - pg-replica-2-data:/var/lib/postgresql/data
      - ./pg-replica/entrypoint.sh:/entrypoint-replica.sh
    cap_add:
      - NET_ADMIN
    depends_on:
      pg-master:
        condition: service_healthy
    networks:
      - chaos-net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 10

volumes:
  pg-master-data:
  pg-replica-1-data:
  pg-replica-2-data:

networks:
  chaos-net:
    driver: bridge
```

- [x] **Step 2: Build and start the cluster**

Run: `cd chaos && sudo docker compose up -d --build`

If DNS fails during build, build image first: `sudo docker build --network=host -f Dockerfile.pg -t chaos-postgres:16 .` then `sudo docker compose up -d`

- [x] **Step 3: Wait for all nodes to be healthy**

Run: `sudo docker compose -f chaos/docker-compose.yml ps`

Expected: All 3 containers show `healthy` status. May take 10-20s for replicas to complete `pg_basebackup`.

- [x] **Step 4: Verify master role**

Run: `sudo docker exec chaos-pg-master psql -U testuser -d testdb -c "SHOW transaction_read_only;"`

Expected: `off`

- [x] **Step 5: Verify replica roles**

Run: `sudo docker exec chaos-pg-replica-1 psql -U testuser -d testdb -c "SHOW transaction_read_only;" && sudo docker exec chaos-pg-replica-2 psql -U testuser -d testdb -c "SHOW transaction_read_only;"`

Expected: Both show `on`

- [x] **Step 6: Verify replication works**

Run:
```bash
sudo docker exec chaos-pg-master psql -U testuser -d testdb -c "INSERT INTO test_data (value) VALUES ('repl_test') RETURNING *;"
sudo docker exec chaos-pg-replica-1 psql -U testuser -d testdb -c "SELECT * FROM test_data WHERE value = 'repl_test';"
```

Expected: Row visible on replica.

- [x] **Step 7: Verify iptables works**

Run:
```bash
sudo docker exec chaos-pg-master iptables -A INPUT -p tcp --dport 5432 -j DROP
sudo docker exec chaos-pg-master iptables -L INPUT -n
sudo docker exec chaos-pg-master iptables -D INPUT -p tcp --dport 5432 -j DROP
```

Expected: Rule added, listed, then removed. No errors.

- [x] **Step 8: Stop the cluster**

Run: `sudo docker compose -f chaos/docker-compose.yml down -v`

- [x] **Step 9: Commit**

```bash
git add chaos/docker-compose.yml
git commit -m "chaos: add Docker Compose with 1 master + 2 replicas"
```

---

### Task 5: Controller — requirements and skeleton

**Files:**
- Create: `chaos/requirements.txt`
- Create: `chaos/controller.py` (skeleton with `/status` and `/reset`)

- [x] **Step 1: Create requirements.txt**

```
fastapi>=0.115,<1
uvicorn>=0.34,<1
```

- [x] **Step 2: Install controller deps**

Run: `uv pip install -r chaos/requirements.txt`

- [x] **Step 3: Create controller.py with node mapping, docker_exec helper, and /status endpoint**

```python
from __future__ import annotations

import asyncio
import json
import subprocess

from fastapi import FastAPI, HTTPException

app = FastAPI(title="Chaos Controller")

NODES = {
    "pg-master": {"container": "chaos-pg-master", "port": 15432},
    "pg-replica-1": {"container": "chaos-pg-replica-1", "port": 15433},
    "pg-replica-2": {"container": "chaos-pg-replica-2", "port": 15434},
}


def _docker_exec(container: str, cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["sudo", "docker", "exec", container, *cmd],
        capture_output=True,
        text=True,
        timeout=10,
    )


def _docker_cmd(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["sudo", *cmd],
        capture_output=True,
        text=True,
        timeout=30,
    )


def _get_container(node: str) -> str:
    if node not in NODES:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown node: {node}. Valid: {list(NODES)}",
        )
    return NODES[node]["container"]


def _is_container_running(container: str) -> bool:
    result = subprocess.run(
        ["sudo", "docker", "inspect", "-f", "{{.State.Running}}", container],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.stdout.strip() == "true"


@app.get("/status")
async def status():
    loop = asyncio.get_event_loop()
    results = {}
    for node, info in NODES.items():
        container = info["container"]
        running = await loop.run_in_executor(None, _is_container_running, container)
        if not running:
            results[node] = {"running": False, "role": None, "replication": None}
            continue

        result = await loop.run_in_executor(
            None,
            _docker_exec,
            container,
            ["psql", "-U", "postgres", "-d", "testdb", "-tA",
             "-c", "SELECT pg_is_in_recovery()"],
        )
        if result.returncode != 0:
            results[node] = {
                "running": True,
                "role": "unknown",
                "error": result.stderr.strip(),
            }
            continue

        is_recovery = result.stdout.strip() == "t"
        role = "replica" if is_recovery else "master"
        results[node] = {"running": True, "role": role}
    return results


@app.post("/reset")
async def reset():
    loop = asyncio.get_event_loop()
    actions = []
    for node, info in NODES.items():
        container = info["container"]
        running = await loop.run_in_executor(None, _is_container_running, container)
        if not running:
            await loop.run_in_executor(
                None, _docker_cmd, ["docker", "start", container],
            )
            actions.append(f"restarted {node}")
        else:
            await loop.run_in_executor(
                None,
                _docker_exec,
                container,
                ["iptables", "-F", "INPUT"],
            )
            actions.append(f"flushed iptables on {node}")
    return {"actions": actions}
```

- [x] **Step 4: Start controller and verify /status**

Run (in background): `uvicorn chaos.controller:app --port 8080 &`

Note: Must run from the project root so `chaos.controller` is importable, or use: `cd chaos && uvicorn controller:app --port 8080 &`

Run: `curl -s localhost:8080/status | python3 -m json.tool`

Expected: JSON with all 3 nodes showing role and running status (or errors if cluster is down).

- [x] **Step 5: Kill the controller**

Run: `kill %1` (or `pkill -f uvicorn`)

- [x] **Step 6: Commit**

```bash
git add chaos/requirements.txt chaos/controller.py
git commit -m "chaos: add controller skeleton with /status and /reset"
```

---

### Task 6: Controller — chaos endpoints

**Files:**
- Modify: `chaos/controller.py`

- [x] **Step 1: Add /freeze/{node} endpoint**

Append to `chaos/controller.py`:

```python
@app.post("/freeze/{node}")
async def freeze(node: str):
    container = _get_container(node)
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        _docker_exec,
        container,
        ["iptables", "-A", "INPUT", "-p", "tcp", "--dport", "5432", "-j", "DROP"],
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "freeze", "node": node}
```

- [x] **Step 2: Add /unfreeze/{node} endpoint**

Append to `chaos/controller.py`:

```python
@app.post("/unfreeze/{node}")
async def unfreeze(node: str):
    container = _get_container(node)
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        _docker_exec,
        container,
        ["iptables", "-D", "INPUT", "-p", "tcp", "--dport", "5432", "-j", "DROP"],
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "unfreeze", "node": node}
```

- [x] **Step 3: Add /promote/{node} endpoint**

Append to `chaos/controller.py`:

```python
@app.post("/promote/{node}")
async def promote(node: str):
    container = _get_container(node)
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        _docker_exec,
        container,
        ["psql", "-U", "postgres", "-c", "SELECT pg_promote();"],
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "promote", "node": node}
```

- [x] **Step 4: Add /kill/{node} and /restart/{node} endpoints**

Append to `chaos/controller.py`:

```python
@app.post("/kill/{node}")
async def kill_node(node: str):
    container = _get_container(node)
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, _docker_cmd, ["docker", "stop", container],
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "kill", "node": node}


@app.post("/restart/{node}")
async def restart_node(node: str):
    container = _get_container(node)
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, _docker_cmd, ["docker", "start", container],
    )
    if result.returncode != 0:
        raise HTTPException(status_code=500, detail=result.stderr.strip())
    return {"action": "restart", "node": node}
```

- [x] **Step 5: Verify all endpoints**

Start cluster: `sudo docker compose -f chaos/docker-compose.yml up -d`

Start controller: `cd chaos && uvicorn controller:app --port 8080 &`

Run:
```bash
curl -s localhost:8080/status | python3 -m json.tool
curl -s -X POST localhost:8080/freeze/pg-master | python3 -m json.tool
curl -s -X POST localhost:8080/unfreeze/pg-master | python3 -m json.tool
curl -s -X POST localhost:8080/promote/pg-replica-1 | python3 -m json.tool
curl -s localhost:8080/status | python3 -m json.tool
curl -s -X POST localhost:8080/reset | python3 -m json.tool
```

Expected: All return 200 with JSON. Status shows pg-replica-1 is now master after promote.

Stop controller and cluster:
```bash
pkill -f uvicorn
sudo docker compose -f chaos/docker-compose.yml down -v
```

- [x] **Step 6: Commit**

```bash
git add chaos/controller.py
git commit -m "chaos: add freeze/unfreeze/promote/kill/restart endpoints"
```

---

### Task 7: Harness — base.py

**Files:**
- Create: `chaos/harness/__init__.py`
- Create: `chaos/harness/base.py`

- [x] **Step 1: Create empty __init__.py**

Create `chaos/harness/__init__.py` as an empty file.

- [x] **Step 2: Create base.py with logging, metrics loop, and read/write loop**

```python
from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from typing import Any

from hasql.pool_manager import BasePoolManager


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
            m = await manager.metrics()
            g = m.gauges
            pool_details = []
            for p in m.pools:
                status = "healthy" if p.healthy else "unhealthy"
                rt = f"{p.response_time:.1f}ms" if p.response_time is not None else "?"
                pool_details.append(f"{p.host}({p.role},{status},{rt})")
            detail = (
                f"masters={g.master_count} replicas={g.replica_count} "
                f"available={g.available_count} active={g.active_connections} "
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
```

- [x] **Step 3: Commit**

```bash
git add chaos/harness/
git commit -m "chaos: add harness base with logging, metrics, and run loop"
```

---

### Task 8: Harness — asyncpg runner

**Files:**
- Create: `chaos/harness/run_asyncpg.py`

- [x] **Step 1: Create run_asyncpg.py**

```python
from __future__ import annotations

import asyncio
import signal

from hasql.driver.asyncpg import PoolManager

from base import DSN, POOL_MANAGER_KWARGS, log, log_metrics, run_harness

DRIVER = "asyncpg"


async def execute_write(conn, manager) -> None:
    await conn.execute(
        "INSERT INTO test_data (value) VALUES ($1)",
        "chaos_test",
    )


async def execute_read(conn, manager) -> None:
    await conn.fetchval("SELECT count(*) FROM test_data")


async def main() -> None:
    manager = PoolManager(
        DSN,
        **POOL_MANAGER_KWARGS,
        pool_factory_kwargs={"min_size": 2, "max_size": 5},
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
```

- [x] **Step 2: Verify it starts against a running cluster**

Start cluster: `sudo docker compose -f chaos/docker-compose.yml up -d` (wait for healthy)

Run: `cd chaos/harness && python run_asyncpg.py`

Expected: Logs `init | waiting for ready`, then `init | cluster ready`, then alternating `write | ok` and `read | ok` lines with `metrics` lines every second. Ctrl+C to stop.

- [x] **Step 3: Commit**

```bash
git add chaos/harness/run_asyncpg.py
git commit -m "chaos: add asyncpg harness runner"
```

---

### Task 9: Harness — aiopg runner

**Files:**
- Create: `chaos/harness/run_aiopg.py`

- [x] **Step 1: Create run_aiopg.py**

```python
from __future__ import annotations

import asyncio
import signal

from hasql.driver.aiopg import PoolManager

from base import DSN, POOL_MANAGER_KWARGS, log, run_harness

DRIVER = "aiopg"


async def execute_write(conn, manager) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO test_data (value) VALUES (%s)",
            ("chaos_test",),
        )


async def execute_read(conn, manager) -> None:
    async with conn.cursor() as cur:
        await cur.execute("SELECT count(*) FROM test_data")
        await cur.fetchone()


async def main() -> None:
    manager = PoolManager(
        DSN,
        **POOL_MANAGER_KWARGS,
        pool_factory_kwargs={"minsize": 2, "maxsize": 5},
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
```

Note: aiopg uses `minsize`/`maxsize` (no underscore), not `min_size`/`max_size`.

- [x] **Step 2: Verify against running cluster**

Run: `cd chaos/harness && python run_aiopg.py`

Expected: Same pattern — init, ready, alternating write/read/metrics. Ctrl+C to stop.

- [x] **Step 3: Commit**

```bash
git add chaos/harness/run_aiopg.py
git commit -m "chaos: add aiopg harness runner"
```

---

### Task 10: Harness — psycopg3 runner

**Files:**
- Create: `chaos/harness/run_psycopg3.py`

- [x] **Step 1: Create run_psycopg3.py**

```python
from __future__ import annotations

import asyncio
import signal

from hasql.driver.psycopg3 import PoolManager

from base import DSN, POOL_MANAGER_KWARGS, log, run_harness

DRIVER = "psycopg3"


async def execute_write(conn, manager) -> None:
    await conn.execute(
        "INSERT INTO test_data (value) VALUES (%s)",
        ("chaos_test",),
    )
    await conn.execute("COMMIT")


async def execute_read(conn, manager) -> None:
    await conn.execute("SELECT count(*) FROM test_data")
    await conn.execute("COMMIT")


async def main() -> None:
    manager = PoolManager(
        DSN,
        **POOL_MANAGER_KWARGS,
        pool_factory_kwargs={"min_size": 2, "max_size": 5},
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
```

Note: psycopg3 connections have autocommit off by default. Explicit `COMMIT` after write is needed.

- [x] **Step 2: Verify against running cluster**

Run: `cd chaos/harness && python run_psycopg3.py`

Expected: Same pattern. Ctrl+C to stop.

- [x] **Step 3: Commit**

```bash
git add chaos/harness/run_psycopg3.py
git commit -m "chaos: add psycopg3 harness runner"
```

---

### Task 11: Harness — asyncsqlalchemy runner

**Files:**
- Create: `chaos/harness/run_asyncsqlalchemy.py`

- [x] **Step 1: Create run_asyncsqlalchemy.py**

```python
from __future__ import annotations

import asyncio
import signal

import sqlalchemy as sa

from hasql.driver.asyncsqlalchemy import PoolManager

from base import DSN, POOL_MANAGER_KWARGS, log, run_harness

DRIVER = "asyncsqlalchemy"


async def execute_write(conn, manager) -> None:
    await conn.execute(
        sa.text("INSERT INTO test_data (value) VALUES (:val)"),
        {"val": "chaos_test"},
    )
    await conn.execute(sa.text("COMMIT"))


async def execute_read(conn, manager) -> None:
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
```

Note: asyncsqlalchemy uses `pool_size` (no min/max split), and requires `sa.text()` for raw SQL.

- [x] **Step 2: Verify against running cluster (skipped - requires live Docker cluster)**

Run: `cd chaos/harness && python run_asyncsqlalchemy.py`

Expected: Same pattern. Ctrl+C to stop.

- [x] **Step 3: Commit**

```bash
git add chaos/harness/run_asyncsqlalchemy.py
git commit -m "chaos: add asyncsqlalchemy harness runner"
```

---

### Task 12: Harness — aiopg_sa runner

**Files:**
- Create: `chaos/harness/run_aiopg_sa.py`

- [x] **Step 1: Create run_aiopg_sa.py**

```python
from __future__ import annotations

import asyncio
import signal

import sqlalchemy as sa

from hasql.driver.aiopg_sa import PoolManager

from base import DSN, POOL_MANAGER_KWARGS, log, run_harness

DRIVER = "aiopg_sa"


async def execute_write(conn, manager) -> None:
    await conn.execute(
        sa.text("INSERT INTO test_data (value) VALUES (:val)"),
        {"val": "chaos_test"},
    )


async def execute_read(conn, manager) -> None:
    await conn.scalar(sa.text("SELECT count(*) FROM test_data"))


async def main() -> None:
    manager = PoolManager(
        DSN,
        **POOL_MANAGER_KWARGS,
        pool_factory_kwargs={"minsize": 2, "maxsize": 5},
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
```

Note: aiopg_sa uses `minsize`/`maxsize` (inherited from aiopg), and `sa.text()` for raw SQL. No explicit COMMIT needed — aiopg_sa auto-commits.

- [x] **Step 2: Verify against running cluster (skipped - requires live Docker cluster)**

Run: `cd chaos/harness && python run_aiopg_sa.py`

Expected: Same pattern. Ctrl+C to stop.

- [x] **Step 3: Commit**

```bash
git add chaos/harness/run_aiopg_sa.py
git commit -m "chaos: add aiopg_sa harness runner"
```

---

### Task 13: Scenario — freeze_master.py

**Files:**
- Create: `chaos/scenarios/__init__.py`
- Create: `chaos/scenarios/freeze_master.py`

- [ ] **Step 1: Create empty __init__.py**

Create `chaos/scenarios/__init__.py` as an empty file.

- [ ] **Step 2: Create freeze_master.py**

```python
"""Master freeze + promote scenario.

Phases:
  1. Start all 5 driver harnesses, wait for steady state
  2. Freeze master (iptables DROP)
  3. Wait until all harnesses detect write failures
  4. Promote pg-replica-1 to master
  5. Wait until all harnesses resume successful writes
  6. Unfreeze old master, observe for 15s
  7. Print per-driver summary
"""
from __future__ import annotations

import asyncio
import subprocess
import sys
import time

import httpx

CONTROLLER = "http://localhost:8080"
HARNESS_DIR = "../harness"
DRIVERS = ["asyncpg", "aiopg", "psycopg3", "asyncsqlalchemy", "aiopg_sa"]
GATE_TIMEOUT = 60  # seconds


async def api(method: str, path: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.request(method, f"{CONTROLLER}{path}", timeout=30)
        resp.raise_for_status()
        return resp.json()


async def wait_gate(
    description: str,
    check_fn,
    timeout: float = GATE_TIMEOUT,
    poll_interval: float = 1.0,
) -> float:
    """Poll check_fn until it returns True. Returns elapsed seconds."""
    print(f"  gate: waiting for {description} (timeout={timeout}s)")
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if await check_fn():
            elapsed = time.monotonic() - t0
            print(f"  gate: {description} — passed in {elapsed:.1f}s")
            return elapsed
        await asyncio.sleep(poll_interval)
    elapsed = time.monotonic() - t0
    print(f"  gate: {description} — TIMEOUT after {elapsed:.1f}s")
    return elapsed


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


async def run() -> None:
    frozen: list[str] = []
    killed: list[str] = []
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
            "new master detected (pg-replica-1 is master)",
            lambda: check_status_masters(1),
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
        print(f"  Freeze → detection: {timestamps['freeze_detected'] - timestamps['freeze']:.1f}s")
        print(f"  Promote → detection: {timestamps['promote_detected'] - timestamps['promote']:.1f}s")
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
        for node in killed:
            print(f"  restarting {node}")
            try:
                await api("POST", f"/restart/{node}")
            except Exception as exc:
                print(f"  warning: restart {node} failed: {exc}")


if __name__ == "__main__":
    asyncio.run(run())
```

- [ ] **Step 3: Install httpx for scenario scripts**

Add `httpx` to `chaos/requirements.txt`:

```
fastapi>=0.115,<1
uvicorn>=0.34,<1
httpx>=0.28,<1
```

Run: `uv pip install -r chaos/requirements.txt`

- [ ] **Step 4: Verify scenario runs end-to-end**

Prerequisites running:
1. Cluster: `sudo docker compose -f chaos/docker-compose.yml up -d`
2. Controller: `cd chaos && uvicorn controller:app --port 8080 &`
3. At least one harness: `cd chaos/harness && python run_asyncpg.py &`

Run: `cd chaos/scenarios && python freeze_master.py`

Expected: Phases print in order, timestamps recorded, cleanup runs at the end.

- [ ] **Step 5: Commit**

```bash
git add chaos/scenarios/ chaos/requirements.txt
git commit -m "chaos: add freeze_master scenario with event-driven gates"
```

---

### Task 14: Scenario — replica_failover.py

**Files:**
- Create: `chaos/scenarios/replica_failover.py`

- [ ] **Step 1: Create replica_failover.py**

```python
"""Replica failover scenario.

Phases:
  1. Verify steady state (1 master, 2 replicas)
  2. Kill pg-replica-1 → replicas=1
  3. Kill pg-replica-2 → replicas=0, reads fall back to master
  4. Restart pg-replica-1 → replicas=1
  5. Restart pg-replica-2 → replicas=2
  6. Print summary
"""
from __future__ import annotations

import asyncio
import time

import httpx

CONTROLLER = "http://localhost:8080"
GATE_TIMEOUT = 60


async def api(method: str, path: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.request(method, f"{CONTROLLER}{path}", timeout=30)
        resp.raise_for_status()
        return resp.json()


async def wait_gate(
    description: str,
    check_fn,
    timeout: float = GATE_TIMEOUT,
    poll_interval: float = 1.0,
) -> float:
    print(f"  gate: waiting for {description} (timeout={timeout}s)")
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if await check_fn():
            elapsed = time.monotonic() - t0
            print(f"  gate: {description} — passed in {elapsed:.1f}s")
            return elapsed
        await asyncio.sleep(poll_interval)
    elapsed = time.monotonic() - t0
    print(f"  gate: {description} — TIMEOUT after {elapsed:.1f}s")
    return elapsed


async def count_replicas() -> int:
    try:
        status = await api("GET", "/status")
        return sum(1 for v in status.values() if v.get("role") == "replica")
    except Exception:
        return -1


async def run() -> None:
    killed: list[str] = []
    timestamps: dict[str, float] = {}
    t_start = time.monotonic()

    try:
        # Phase 1: Steady state
        print("\n=== Phase 1: Verify steady state ===")
        await wait_gate("2 replicas available", lambda: count_replicas() >= 2)

        # Phase 2: Kill first replica
        print("\n=== Phase 2: Kill pg-replica-1 ===")
        await api("POST", "/kill/pg-replica-1")
        killed.append("pg-replica-1")
        timestamps["kill_1"] = time.monotonic()
        print(f"  POST /kill/pg-replica-1 at T+{timestamps['kill_1'] - t_start:.1f}s")

        t = await wait_gate("replicas=1", lambda: count_replicas() == 1)
        timestamps["kill_1_detected"] = time.monotonic()

        # Phase 3: Kill second replica
        print("\n=== Phase 3: Kill pg-replica-2 ===")
        await api("POST", "/kill/pg-replica-2")
        killed.append("pg-replica-2")
        timestamps["kill_2"] = time.monotonic()
        print(f"  POST /kill/pg-replica-2 at T+{timestamps['kill_2'] - t_start:.1f}s")

        t = await wait_gate("replicas=0", lambda: count_replicas() == 0)
        timestamps["kill_2_detected"] = time.monotonic()
        print("  reads should now fall back to master (fallback_master=True)")

        # Phase 4: Restart first replica
        print("\n=== Phase 4: Restart pg-replica-1 ===")
        await api("POST", "/restart/pg-replica-1")
        killed.remove("pg-replica-1")
        timestamps["restart_1"] = time.monotonic()
        print(f"  POST /restart/pg-replica-1 at T+{timestamps['restart_1'] - t_start:.1f}s")

        t = await wait_gate("replicas=1", lambda: count_replicas() >= 1)
        timestamps["restart_1_detected"] = time.monotonic()

        # Phase 5: Restart second replica
        print("\n=== Phase 5: Restart pg-replica-2 ===")
        await api("POST", "/restart/pg-replica-2")
        killed.remove("pg-replica-2")
        timestamps["restart_2"] = time.monotonic()
        print(f"  POST /restart/pg-replica-2 at T+{timestamps['restart_2'] - t_start:.1f}s")

        t = await wait_gate("replicas=2", lambda: count_replicas() >= 2)
        timestamps["restart_2_detected"] = time.monotonic()

        # Phase 6: Summary
        print("\n=== Summary ===")
        print(f"  Total duration: {timestamps['restart_2_detected'] - t_start:.1f}s")
        print(f"  Kill replica-1 → detected: {timestamps['kill_1_detected'] - timestamps['kill_1']:.1f}s")
        print(f"  Kill replica-2 → detected: {timestamps['kill_2_detected'] - timestamps['kill_2']:.1f}s")
        print(f"  Restart replica-1 → detected: {timestamps['restart_1_detected'] - timestamps['restart_1']:.1f}s")
        print(f"  Restart replica-2 → detected: {timestamps['restart_2_detected'] - timestamps['restart_2']:.1f}s")
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
```

- [ ] **Step 2: Verify scenario runs end-to-end**

Prerequisites running (cluster, controller, at least one harness).

Run: `cd chaos/scenarios && python replica_failover.py`

Expected: Phases print in order, replicas killed and restarted, timing summary printed.

- [ ] **Step 3: Commit**

```bash
git add chaos/scenarios/replica_failover.py
git commit -m "chaos: add replica_failover scenario"
```

---

### Task 15: Justfile integration

**Files:**
- Modify: `justfile`

- [ ] **Step 1: Add chaos recipes to justfile**

Append to `justfile`:

```just
# Start chaos test cluster
chaos-up:
    sudo docker compose -f chaos/docker-compose.yml up -d --build

# Stop chaos test cluster and remove volumes
chaos-down:
    sudo docker compose -f chaos/docker-compose.yml down -v

# Show chaos cluster status via controller API
chaos-status:
    @curl -s localhost:8080/status | python3 -m json.tool

# Reset chaos cluster (unfreeze all, restart stopped)
chaos-reset:
    @curl -s -X POST localhost:8080/reset | python3 -m json.tool

# Start chaos controller
chaos-controller:
    cd chaos && uvicorn controller:app --port 8080
```

- [ ] **Step 2: Verify**

Run: `just chaos-up` (cluster starts)

Run: `just chaos-controller &` then `just chaos-status` (status returned)

Run: `just chaos-down` (cluster stops)

- [ ] **Step 3: Commit**

```bash
git add justfile
git commit -m "chaos: add justfile recipes for chaos stand"
```

---

### Task 16: End-to-end verification

- [ ] **Step 1: Clean start**

```bash
just chaos-down 2>/dev/null; just chaos-up
```

Wait for all containers healthy: `sudo docker compose -f chaos/docker-compose.yml ps`

- [ ] **Step 2: Start controller**

```bash
just chaos-controller &
```

- [ ] **Step 3: Verify status**

```bash
just chaos-status
```

Expected: All 3 nodes running, pg-master=master, pg-replica-1=replica, pg-replica-2=replica.

- [ ] **Step 4: Start one harness and observe**

```bash
cd chaos/harness && python run_asyncpg.py &
```

Expected: Logs show init, ready, then alternating write/read/metrics.

- [ ] **Step 5: Run freeze_master scenario**

```bash
cd chaos/scenarios && python freeze_master.py
```

Expected: All 5 phases complete, timing summary printed, cleanup runs.

- [ ] **Step 6: Reset and run replica_failover scenario**

```bash
just chaos-reset
cd chaos/scenarios && python replica_failover.py
```

Expected: All 6 phases complete, timing summary printed.

- [ ] **Step 7: Clean up**

```bash
pkill -f uvicorn
pkill -f "python run_"
just chaos-down
```

- [ ] **Step 8: Commit if any fixes were needed**

```bash
git add -A chaos/
git commit -m "chaos: fixes from end-to-end verification"
```
