# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

This project uses [just](https://github.com/casey/just) as a task runner. Install it with `brew install just`.

```bash
# Install all development dependencies
just develop

# Run ruff + mypy
just lint

# Run ruff or mypy individually
just ruff
just mypy

# Run all tests
just test

# Run specific test file or pattern
just test tests/test_utils.py
just test tests/test_utils.py -k "connection_string"

# Run tests using tox
uv run tox -e py310 # Python 3.10
uv run tox -e py311 # Python 3.11
```

## Architecture Overview

**hasql** is a high-availability PostgreSQL connection management library that provides automatic master/replica detection and load balancing across multiple database hosts.

### Core Components

1. **BasePoolManager** (`hasql/base.py`) - Abstract base class that defines the core pooling interface and connection management logic

2. **Driver-Specific Pool Managers:**
   - `hasql.aiopg.PoolManager` - aiopg driver support
   - `hasql.asyncpg.PoolManager` - asyncpg driver support
   - `hasql.psycopg3.PoolManager` - psycopg3 driver support
   - `hasql.asyncsqlalchemy.PoolManager` - SQLAlchemy async support
   - `hasql.aiopg_sa.PoolManager` - aiopg with SQLAlchemy support

3. **Balancer Policies** (`hasql/balancer_policy/`) - Load balancing strategies (see details below)

4. **Connection String Parsing** (`hasql/utils.py`) - Handles multi-host PostgreSQL connection strings with support for:
   - Comma-separated hosts: `postgresql://db1,db2,db3/dbname`
   - Per-host ports: `postgresql://db1:1234,db2:5678/dbname`
   - Global port override: `postgresql://db1,db2:6432/dbname`
   - libpq-style connection strings

5. **Metrics and Monitoring** (`hasql/metrics.py`) - Connection and performance metrics collection

### Key Features

- **Automatic Role Detection:** Continuously monitors each host to determine if it's a master or replica
- **Health Monitoring:** Background tasks check host availability and automatically exclude unhealthy hosts
- **Load Balancing:** Multiple policies for distributing connections across healthy replicas
- **Failover Support:** Automatic fallback to master when replicas are unavailable
- **Multi-Driver Support:** Works with asyncpg, aiopg, psycopg3, and SQLAlchemy

### Connection Flow

1. Parse multi-host DSN string into individual host connections
2. Create connection pools for each host with reserved system connections
3. Background tasks continuously check each host's role (master/replica) and health
4. When acquiring connections, balancer selects appropriate pool based on read_only flag
5. Connections are automatically returned to their respective pools when released

### Balancer Policy Architecture

All balancer policies live in `hasql/balancer_policy/`. The module structure:

- `base.py` — `AbstractBalancerPolicy` ABC and `BaseBalancerPolicy` alias
- `greedy.py` — `GreedyBalancerPolicy`
- `random_weighted.py` — `RandomWeightedBalancerPolicy`
- `round_robin.py` — `RoundRobinBalancerPolicy`
- `__init__.py` — re-exports all of the above

**Class hierarchy:** There is a single base class `AbstractBalancerPolicy` (the old `BaseBalancerPolicy` is kept as a backward-compatible alias). It provides:
- `__init__(pool_manager)` — stores the pool manager reference
- `get_pool(read_only, fallback_master, master_as_replica_weight)` — public entry point that handles `master_as_replica_weight` logic, then delegates to `_get_pool`
- `_get_candidates(read_only, fallback_master, choose_master_as_replica)` — builds the list of eligible pools (replicas, master, or both)
- `_get_pool(...)` — **abstract method**, the only thing subclasses must implement

**Circular import note:** `pool_manager.py` imports `AbstractBalancerPolicy` from `balancer_policy.base` (not through `__init__.py`) to break the cycle: `pool_manager` → `balancer_policy/__init__` → `greedy` → `base` → `pool_manager`. The `base.py` module only imports `pool_manager` under `TYPE_CHECKING`.

**Policies:**

| Policy | Selection strategy | Key method |
|---|---|---|
| `GreedyBalancerPolicy` | Picks pool(s) with the most free connections, then random tie-break | Uses `pool_manager.get_pool_freesize()` |
| `RandomWeightedBalancerPolicy` | Weighted random — faster hosts (lower response time) get higher probability | Uses `pool_manager.get_last_response_time()` and `random.choices` |
| `RoundRobinBalancerPolicy` | Cycles through candidates sequentially, tracking index per `(read_only, choose_master_as_replica)` pair | Maintains `_indexes` defaultdict |

**`RandomWeightedBalancerPolicy` weight computation (`_compute_weights`):**
- Receives response times (possibly `None` for unknown hosts)
- `None` is treated as `0` (unknown = assume fast)
- Reflects values around `max_time`: `weight = max_time - time + 1`
- The `+1` ensures all-zero/all-`None` cases produce uniform positive weights (required by `random.choices`)
- No manual normalization — `random.choices` handles that internally

**Default policy:** When no `balancer_policy` is specified, `BasePoolManager.__init__` defaults to `GreedyBalancerPolicy` (resolved via lazy import to avoid circular import).

### Testing Strategy

- Uses pytest with aiomisc test framework
- Mocks database connections for unit testing (`tests/mocks/`)
- Integration tests for each driver implementation
- Coverage reporting with pytest-cov
- Tests are organized by driver type and functionality

## Important Notes

- The codebase uses Python 3.10+ with async/await throughout
- All pool managers extend the abstract `BasePoolManager` class
- Connection strings support both single and multi-host PostgreSQL URLs
- Background health checking runs every `refresh_delay` seconds (default: 1s)
- System reserves one connection per pool for health monitoring
- The library automatically detects PostgreSQL role changes with slight delay
