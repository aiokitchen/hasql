# Chaos Test Stand

Docker-based PostgreSQL chaos testing for hasql drivers. Runs 3 PG nodes (1 master + 2 replicas) with streaming replication and a FastAPI controller for triggering failover scenarios.

## Prerequisites

- Docker with Compose plugin
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- `sudo` access (for docker and iptables inside containers)

## Setup

```bash
cd chaos

# Create venv and install dependencies
uv venv .venv
uv pip install fastapi uvicorn httpx asyncpg aiopg "psycopg[binary]" psycopg-pool "sqlalchemy<2" packaging
uv pip install -e ..  # install hasql itself

# Build the postgres image (needs --network=host if DNS is restricted)
sudo docker build --network=host -f Dockerfile.pg . -t chaos-postgres:16

# Start the cluster
sudo docker compose up -d

# Start the chaos controller
.venv/bin/uvicorn controller:app --port 8081
```

Verify the cluster is ready:

```bash
curl -s localhost:8081/status | python3 -m json.tool
```

Expected output: 1 master on port 15432, 2 replicas on 15433/15434.

## Run All Tests

With the controller running in a separate terminal:

```bash
.venv/bin/python run_all.py
```

This runs all 5 drivers (asyncpg, aiopg, psycopg3, aiopg_sa, asyncsqlalchemy) against both scenarios (freeze_master, replica_failover) and prints a summary report. Results are also saved to `/tmp/chaos_results.json`.

Takes ~2 minutes total.

## Run Individual Scenarios

Start a harness in one terminal:

```bash
cd harness
../.venv/bin/python run_asyncpg.py
```

Trigger a scenario in another:

```bash
cd scenarios
../.venv/bin/python freeze_master.py
../.venv/bin/python replica_failover.py
```

## Scenarios

**freeze_master** -- Blocks all traffic to master via iptables, promotes a replica, then unfreezes the old master. Tests master failover detection and write rerouting.

**replica_failover** -- Kills both replicas sequentially, then restarts them. Tests read fallback to master and replica recovery detection.

## Controller API

| Endpoint | Method | Description |
|---|---|---|
| `/status` | GET | Node roles and health |
| `/freeze/{node}` | POST | Block PG port via iptables |
| `/unfreeze/{node}` | POST | Remove iptables block |
| `/promote/{node}` | POST | `pg_promote()` on a replica |
| `/kill/{node}` | POST | `docker stop` the container |
| `/restart/{node}` | POST | `docker start` the container |
| `/reset` | POST | Unfreeze all + restart stopped nodes |

Node names: `pg-master`, `pg-replica-1`, `pg-replica-2`.

## Cleanup

```bash
sudo docker compose down -v
```
