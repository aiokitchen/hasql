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

### Class Responsibilities

**Call chain:** `BasePoolManager` → `PoolState` → `PoolDriver`. The manager never calls the driver directly.

**PoolState** (`hasql/pool_state.py`) — owns the driver and all pool state.
- **Driver gateway:** single point of contact with the driver. All driver operations (acquire, release, pool factory, close, terminate, is_master, host, stats, freesize) go through PoolState.
- **Pool lifecycle:** creates pools (`pool_factory`), closes/terminates them, stores `pool_factory_kwargs`.
- **Role state:** master/replica/stale sets, role refresh (`refresh_pool_role`), set mutations.
- **Staleness state:** stale pool set (`_stale_pool_set`), check result cache (`_last_check_result`), staleness checking (`check_replica_staleness`), master state collection (`collect_master_state`).
- **Readiness & waiting:** DSN ready events, conditions for master/replica availability.
- **Pool queries:** freesize, response time (stopwatch), pool stats, host, connection status.
- Implements `PoolStateProvider` protocol used by balancer policies.

**BasePoolManager** (`hasql/pool_manager.py`) — thin orchestrator, owns no driver reference.
- **Public API (8 methods/properties):** `acquire()`, `acquire_master()`, `acquire_replica()`, `close()`, `metrics()`, `ready()`, `wait_masters_ready()`, `available_pool_count`.
- **Configuration:** timeouts, delays, fallback, master-as-replica weight, staleness policy (all private: `_acquire_timeout`, `_refresh_delay`, etc.).
- **Connection tracking:** `_unmanaged_connections` registry, `_register_connection`/`_unregister_connection`.
- **Balancer ownership:** creates and holds `_balancer` instance.
- **Metrics aggregation:** combines `PoolState` data with `_unmanaged_connections` tracking.
- **Lifecycle coordination:** `close()` orchestrates health stop, connection cleanup, pool shutdown.
- **Pool state:** accessed via `_pool_state` (private). Commonly-used methods (`ready`, `wait_masters_ready`, `available_pool_count`) are proxied on the manager.

**PoolHealthMonitor** (`hasql/health.py`) — background health checking.
- Spawns one task per DSN: creates pool, acquires sys connection, calls `manager._periodic_pool_check`.
- Uses `manager._pool_state` for pool operations (acquire, release, factory).
- Uses `manager._closing`, `manager._refresh_timeout`, `manager._refresh_delay` for configuration.
- Error recovery: removes unhealthy pools from sets, retries pool creation.

**PoolHealthMonitor** also performs staleness checks: `_full_pool_check` calls `collect_master_state` on master pools and `check_replica_staleness` on replica pools each cycle.

**PoolDriver** (`hasql/abc.py`) — database driver interface.
- Abstract methods for pool/connection operations, including `fetch_scalar` for staleness queries. Implementations in `hasql/driver/`.
- Never called directly by manager or health monitor — always through `PoolState`.

**BalancerPolicy** (`hasql/balancer_policy/`) — pool selection strategies.
- Depends on `PoolStateProvider` protocol (no import of manager).
- Subclasses implement `_get_pool` to select from candidates.

### Other Components

1. **Staleness Detection** (`hasql/staleness.py`) — replication lag checking.
   - `BaseStalenessChecker` ABC with two implementations: `BytesStalenessChecker` (WAL byte lag via `pg_wal_lsn_diff`) and `TimeStalenessChecker` (replay timestamp lag via `pg_last_xact_replay_timestamp`).
   - `CheckContext` — pre-bound query context created by `PoolState`, wraps connection + driver's `fetch_scalar`.
   - `StalenessPolicy` — user-facing wrapper that adds `grace_period` logic (keeps a pool "fresh" for a configurable duration after first stale detection).
   - Integration: `PoolState` owns the `StalenessPolicy` instance. Three pool sets: `_master_pool_set`, `_replica_pool_set`, `_stale_pool_set`. Stale pools are removed from replica set and added to stale set.

2. **Exceptions** (`hasql/exceptions.py`) — custom exception hierarchy:
   - `HasqlError` — base exception for all hasql errors
   - `PoolManagerClosedError` — raised by `acquire()` on closed/closing manager (replaces `RuntimeError`)
   - `PoolManagerClosingError` — raised during shutdown when pool creation exhausted
   - `NoAvailablePoolError` / `UnexpectedDatabaseResponseError` — reserved for future use

3. **Constants** (`hasql/constants.py`) — centralized default values for pool manager configuration (timeouts, delays, weights, window sizes).

4. **Driver-Specific Pool Managers** (`hasql/driver/`):
   - `hasql.aiopg.PoolManager` - aiopg driver support
   - `hasql.asyncpg.PoolManager` - asyncpg driver support
   - `hasql.psycopg3.PoolManager` - psycopg3 driver support
   - `hasql.asyncsqlalchemy.PoolManager` - SQLAlchemy async support
   - `hasql.aiopg_sa.PoolManager` - aiopg with SQLAlchemy support

5. **Connection String Parsing** (`hasql/utils.py`) - Handles multi-host PostgreSQL connection strings with support for:
   - Comma-separated hosts: `postgresql://db1,db2,db3/dbname`
   - Per-host ports: `postgresql://db1:1234,db2:5678/dbname`
   - Global port override: `postgresql://db1,db2:6432/dbname`
   - libpq-style connection strings

6. **Metrics and Monitoring** (`hasql/metrics.py`) - Three-layer metrics model:
   - `PoolStats` — raw per-pool stats returned by `PoolDriver.pool_stats()` (min, max, idle, used, extra)
   - `PoolMetrics` — enriched per-pool metrics built by `BasePoolManager.metrics()`, adding role, healthy, response_time, in_flight, and driver-specific extras
   - `PoolRole` — enum (`MASTER`, `REPLICA`) for type-safe role identification in `PoolMetrics.role`
   - `PoolStaleness` — enum (`FRESH`, `STALE`) for per-pool staleness status in `PoolMetrics.staleness`
   - `PoolMetrics` also includes `staleness: PoolStaleness | None` and `lag: dict[str, Any]` for replication lag data
   - `HasqlGauges` — point-in-time snapshot of manager state (master_count, replica_count, stale_count, unavailable_count, active_connections, closing, closed)
   - `Metrics` — top-level container with `pools: Sequence[PoolMetrics]`, `hasql: HasqlMetrics`, `gauges: HasqlGauges`. The old `drivers` field is a deprecated backward-compat property.
   - `PoolState` owns the pool state used for enrichment (role, health, response time); `BasePoolManager` owns `_unmanaged_connections` and `_metrics`
   - Drivers implement `pool_stats(pool) -> PoolStats` (the old `driver_metrics()` is deprecated with a default that delegates to `pool_stats()`)

### Key Features

- **Automatic Role Detection:** Continuously monitors each host to determine if it's a master or replica
- **Health Monitoring:** Background tasks check host availability and automatically exclude unhealthy hosts
- **Load Balancing:** Multiple policies for distributing connections across healthy replicas
- **Staleness Detection:** Monitors replication lag and automatically excludes stale replicas with tiered fallback (fresh replicas > master > stale replicas > wait)
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
- `_get_candidates(read_only, fallback_master, choose_master_as_replica)` — builds the list of eligible pools with tiered fallback for `read_only=True`: (1) fresh replicas, (2) master if `fallback_master=True`, (3) stale replicas, (4) wait for fresh replicas
- `_get_pool(...)` — **abstract method**, the only thing subclasses must implement

**Import architecture:** The circular import between `pool_manager` and `balancer_policy` is broken by the `PoolStateProvider` protocol in `pool_state.py`. Balancer policies depend on `PoolStateProvider` (direct import from `pool_state.py`, no `TYPE_CHECKING` needed). `BasePoolManager` passes its `_pool_state` attribute (which implements `PoolStateProvider`) to the balancer constructor. Import graph (no cycles): `pool_state.py` → `utils.py`, `abc.py`, `acquire.py`, `metrics.py`, `staleness.py`; `balancer_policy/base.py` → `pool_state.py`; `pool_manager.py` → `pool_state.py`, `balancer_policy/`, `health.py`, `staleness.py`; `health.py` → `pool_manager.py` (TYPE_CHECKING only).

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

**Default policy:** When no `balancer_policy` is specified, `BasePoolManager.__init__` defaults to `GreedyBalancerPolicy` (imported directly at module level — no lazy import needed since the cycle is broken).

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
