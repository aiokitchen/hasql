# Migration Guide: hasql 0.9.0 → 0.10.0

## TL;DR

| What you do today | Action needed |
|---|---|
| `from hasql.aiopg import PoolManager` (or asyncpg, psycopg3, etc.) | **None immediately** — compatibility shims remain, but `hasql.driver.*` is preferred for new code |
| `from hasql.base import BasePoolManager, TimeoutAcquireContext` | **None** — re-exports preserved |
| Subclass `BasePoolManager` to add a custom driver | **Rewrite** — extract driver into `PoolDriver` subclass |
| Override `_prepare_acquire_kwargs` | **Rewrite** — use explicit `timeout` parameter |
| Manager pool-state proxies (`get_master_pools`, `pools`, etc.) | **Update** — the manager API intentionally no longer exposes raw `PoolState`; custom integrations should own/use `PoolState` directly |
| Patch `_is_master` / `_pool_factory` in tests | **Update** — patch on driver via `_pool_state.driver` |
| Access `_refresh_role_tasks` | **Update** — use `_health.tasks` |
| Call `_notify_about_pool_has_checked` | **Update** — use `_pool_state.notify_pool_checked` |
| `metrics().drivers` → list of `DriverMetrics` | **Update** — use `metrics().pools` → list of `PoolMetrics` |
| `PoolDriver.driver_metrics(pools)` override | **Update** — implement `pool_stats(pool) -> PoolStats` instead |
| `from hasql.metrics import DriverMetrics` | **None** — still available, but deprecated |
| `metrics().hasql` | **None** — works unchanged |

---

## Architecture: Composition over Inheritance

`BasePoolManager` was an **abstract** class. Each driver (aiopg, asyncpg, etc.)
subclassed it and implemented ~10 abstract methods. Now:

- `BasePoolManager` is **concrete** — it accepts a `driver: PoolDriver` instance
- Driver-specific logic lives in `PoolDriver` subclasses (`AiopgDriver`, `AsyncpgDriver`, etc.)
- Driver-specific `PoolManager` classes are thin wrappers that create the right driver

```
# Before (0.9.0)
BasePoolManager (ABC)
  └── hasql.aiopg.PoolManager  (implements all abstract methods)

# After (0.10.0)
PoolDriver (ABC)
  └── AiopgDriver              (implements driver interface)

BasePoolManager (concrete)     (has-a PoolDriver)
  └── hasql.aiopg.PoolManager  (thin wrapper: passes AiopgDriver)
```

### New modules

| Module | Contains |
|---|---|
| `hasql.abc` | `PoolDriver` ABC |
| `hasql.acquire` | `AcquireContext`, `TimeoutAcquireContext`, `PoolAcquireContext` |
| `hasql.constants` | `DEFAULT_REFRESH_DELAY`, `DEFAULT_ACQUIRE_TIMEOUT`, etc. |
| `hasql.health` | `PoolHealthMonitor` (extracted from BasePoolManager) |
| `hasql.pool_state` | `PoolState`, `PoolStateProvider` protocol |
| `hasql.balancer_policy` | `AbstractBalancerPolicy` |
| `hasql.pool_manager` | `BasePoolManager` (concrete) |
| `hasql.driver.*` | Driver implementations + PoolManager wrappers |

The old driver modules (`hasql.aiopg`, `hasql.asyncpg`, etc.) remain cheap
backward-compatible import shims. New code should import from
`hasql.driver.*`. `hasql.psycopg3.PoolAcquireContext` remains an identity
alias of `Psycopg3AcquireContext`.

---

## Compatibility imports

`hasql.base` is intentionally limited to imports available from the former
module or required by existing direct imports:

```python
from hasql.base import (
    BasePoolManager,
    AbstractBalancerPolicy,
    TimeoutAcquireContext,
    PoolAcquireContext,
)
```

Import `PoolDriver`, `AcquireContext`, `PoolState`, and `PoolStateProvider`
from `hasql.abc`, `hasql.acquire`, and `hasql.pool_state` respectively.
Type variables and new internals are not exported by `hasql.base`.

### Patching driver methods in tests

Driver methods are now on the `PoolDriver` subclass, accessible via
`pool_manager._pool_state.driver`:

```python
# Patch on the driver instance
with mock.patch.object(
    pool_manager._pool_state.driver, "is_master", ...
):
    ...

# Or patch the driver class method
with mock.patch.object(
    AiopgDriver, "is_master", ...
):
    ...
```

---

## Migration required

### 1. Driver import paths

Driver-specific `PoolManager` classes have moved from `hasql.<driver>` to
`hasql.driver.<driver>`. The old modules (`hasql.aiopg`, `hasql.asyncpg`, etc.)
still work via re-export shims but are deprecated.

```python
# Old (0.9.0)                              # New (0.10.0)
from hasql.aiopg import PoolManager        # from hasql.driver.aiopg import PoolManager
from hasql.asyncpg import PoolManager      # from hasql.driver.asyncpg import PoolManager
from hasql.psycopg3 import PoolManager     # from hasql.driver.psycopg3 import PoolManager
from hasql.asyncsqlalchemy import PoolManager  # from hasql.driver.asyncsqlalchemy import PoolManager
from hasql.aiopg_sa import PoolManager     # from hasql.driver.aiopg_sa import PoolManager
from hasql.asyncpgsa import PoolManager    # from hasql.driver.asyncpgsa import PoolManager
```

Usage remains the same after updating the import:

```python
from hasql.driver.asyncpg import PoolManager

async with PoolManager("postgresql://master,replica/db") as pool:
    async with pool.acquire_master() as conn:
        ...
```

### 2. Manager API and pool state

The supported manager API is `acquire`, `acquire_master`, `acquire_replica`,
`release`, `close`, `terminate`, `metrics`, `ready`, `wait_masters_ready`,
`available_pool_count`, and async context-manager methods.

This release intentionally removes raw state/configuration proxies including
`dsn`, role counts, `pools`, lifecycle flags, `balancer`, refresh values,
role predicates, pool statistics helpers, raw pool getters, and replica/all
health-wait methods. Libraries implementing custom orchestration should
compose a `PoolState` directly instead of reaching through a manager. This is
an allowed manager API break in 0.10.0.

### 3. Custom BasePoolManager subclasses

**Before (0.9.0):** You subclassed `BasePoolManager` and implemented abstract methods.

```python
# OLD — will NOT work in 0.10.0
from hasql.base import BasePoolManager, TimeoutAcquireContext

class MyPoolManager(BasePoolManager):
    def get_pool_freesize(self, pool):
        return pool.freesize

    def acquire_from_pool(self, pool, **kwargs):
        timeout = kwargs.pop("_timeout", None)
        ctx = pool.acquire(**kwargs)
        if timeout is not None:
            return TimeoutAcquireContext(ctx, timeout)
        return ctx

    async def release_to_pool(self, connection, pool, **kwargs):
        await pool.release(connection, **kwargs)

    async def _is_master(self, connection):
        return await connection.fetchrow("SHOW transaction_read_only")[0] == "off"

    async def _pool_factory(self, dsn):
        return await my_driver.create_pool(str(dsn), **self.pool_factory_kwargs)

    def _prepare_pool_factory_kwargs(self, kwargs):
        kwargs["min_size"] = kwargs.get("min_size", 1) + 1
        kwargs["max_size"] = kwargs.get("max_size", 10) + 1
        return kwargs

    async def _close(self, pool):
        await pool.close()

    async def _terminate(self, pool):
        pool.terminate()

    def is_connection_closed(self, connection):
        return connection.is_closed()

    def host(self, pool):
        return pool.host

    def _driver_metrics(self):
        return [...]
```

**After (0.10.0):** Extract the driver logic into a `PoolDriver` subclass.
Implement `pool_stats()` instead of `driver_metrics()`.

```python
# NEW — 0.10.0
from hasql.abc import PoolDriver
from hasql.acquire import TimeoutAcquireContext
from hasql.metrics import PoolStats
from hasql.pool_manager import BasePoolManager

class MyDriver(PoolDriver[MyPool, MyConnection]):
    def get_pool_freesize(self, pool):
        return pool.freesize

    def acquire_from_pool(self, pool, *, timeout=None, **kwargs):
        ctx = pool.acquire(**kwargs)
        if timeout is not None:
            return TimeoutAcquireContext(ctx, timeout)
        return ctx

    async def release_to_pool(self, connection, pool, **kwargs):
        await pool.release(connection, **kwargs)

    async def is_master(self, connection):  # was _is_master
        return await connection.fetchrow("SHOW transaction_read_only")[0] == "off"

    async def pool_factory(self, dsn, **kwargs):  # was _pool_factory
        return await my_driver.create_pool(str(dsn), **kwargs)

    def prepare_pool_factory_kwargs(self, kwargs):  # was _prepare_pool_factory_kwargs
        return {
            **kwargs,
            "min_size": kwargs.get("min_size", 1) + 1,
            "max_size": kwargs.get("max_size", 10) + 1,
        }

    async def close_pool(self, pool):  # was _close
        await pool.close()

    async def terminate_pool(self, pool):  # was _terminate
        pool.terminate()

    def is_connection_closed(self, connection):
        return connection.is_closed()

    def host(self, pool):
        return pool.host

    def pool_stats(self, pool) -> PoolStats:  # was _driver_metrics(self)
        return PoolStats(
            min=pool.min, max=pool.max,
            idle=pool.idle, used=pool.size - pool.idle,
        )


class MyPoolManager(BasePoolManager[MyPool, MyConnection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=MyDriver(), **kwargs)
```

### Method name mapping

| Old (on BasePoolManager, 0.9.0) | New (on PoolDriver, 0.10.0) |
|---|---|
| `_is_master(connection)` | `is_master(connection)` |
| `_pool_factory(dsn)` | `pool_factory(dsn, **kwargs)` |
| `_prepare_pool_factory_kwargs(kwargs)` | `prepare_pool_factory_kwargs(kwargs)` |
| `_close(pool)` | `close_pool(pool)` |
| `_terminate(pool)` | `terminate_pool(pool)` |
| `_driver_metrics()` | `pool_stats(pool) -> PoolStats` |
| `get_pool_freesize(pool)` | `get_pool_freesize(pool)` |
| `acquire_from_pool(pool, **kwargs)` | `acquire_from_pool(pool, *, timeout=None, **kwargs)` |
| `release_to_pool(connection, pool)` | `release_to_pool(connection, pool)` |
| `is_connection_closed(connection)` | `is_connection_closed(connection)` |
| `host(pool)` | `host(pool)` |

Key differences:

- **Public names:** `_is_master` → `is_master`, `_close` → `close_pool`, etc.
- **`pool_factory` receives `**kwargs`:** The pool factory kwargs are passed
  as arguments instead of being read from `self.pool_factory_kwargs`.
- **`pool_stats` replaces `driver_metrics`:** Returns `PoolStats` for a single
  pool. The manager handles iteration, None-filtering, and enrichment.
- **`acquire_from_pool` has explicit `timeout`:** Timeout is a dedicated
  keyword argument, not smuggled through `**kwargs`.

### 4. `_prepare_acquire_kwargs` removed

**Before (0.9.0):** Drivers overrode `_prepare_acquire_kwargs` to smuggle the
timeout into `**kwargs` under a driver-specific key (e.g. `_timeout`, `timeout`):

```python
# OLD
class PoolManager(BasePoolManager):
    def _prepare_acquire_kwargs(self, kwargs, timeout):
        prepared = super()._prepare_acquire_kwargs(kwargs, timeout)
        prepared["_timeout"] = timeout
        return prepared

    def acquire_from_pool(self, pool, **kwargs):
        timeout = kwargs.pop("_timeout", None)
        ctx = pool.acquire(**kwargs)
        if timeout is not None:
            return TimeoutAcquireContext(ctx, timeout)
        return ctx
```

**After (0.10.0):** `timeout` is an explicit parameter on `acquire_from_pool`.
No smuggling needed.

```python
# NEW
class MyDriver(PoolDriver[MyPool, MyConnection]):
    def acquire_from_pool(self, pool, *, timeout=None, **kwargs):
        ctx = pool.acquire(**kwargs)
        if timeout is not None:
            return TimeoutAcquireContext(ctx, timeout)
        return ctx
```

### 5. `_refresh_role_tasks` → `_health.tasks`

**Before (0.9.0):**

```python
for task in pool_manager._refresh_role_tasks:
    task.cancel()
```

**After (0.10.0):**

```python
for task in pool_manager._health.tasks:
    task.cancel()
```

Health monitoring logic (background tasks, pool creation retry, role checking)
has been extracted into `PoolHealthMonitor` (`hasql.health`), accessible via
`pool_manager._health`.

### 6. `_notify_about_pool_has_checked` → `_pool_state.notify_pool_checked`

**Before (0.9.0):**

```python
await self._notify_about_pool_has_checked(dsn)
```

**After (0.10.0):**

```python
await self._pool_state.notify_pool_checked(dsn)
```

### 7. Removed public methods and properties

The following have been removed from `BasePoolManager`'s public API in 0.10.0:

| Removed | Replacement |
|---|---|
| `pool.driver` | `pool._pool_state.driver` |
| `pool.pool_factory_kwargs` | `pool._pool_state.pool_factory_kwargs` |
| `pool.host(p)` | `pool._pool_state.host(p)` |
| `pool.is_connection_closed(c)` | `pool._pool_state.is_connection_closed(c)` |
| `pool.acquire_from_pool(p)` | `pool._pool_state.acquire_from_pool(p)` |
| `pool.release_to_pool(c, p)` | `pool._pool_state.release_to_pool(c, p)` |
| `pool.register_connection(c, p)` | Internal `pool._register_connection(c, p)` |
| `pool.unregister_connection(c)` | Internal `pool._unregister_connection(c)` |
| `iter(pool)` | `iter(pool._pool_state)` |
| `pool.refresh_delay` / `pool.refresh_timeout` | Constructor configuration; no public runtime property |
| `pool.balancer` | No manager proxy; custom orchestration should compose `PoolState` |
| `pool.closing` / `pool.closed` | `pool.metrics().gauges.closing` / `pool.metrics().gauges.closed` |

`release(conn)` and `terminate()` remain part of the supported manager API.

### 8. Metrics: `PoolMetrics` replaces `DriverMetrics`

The old `Metrics.drivers` field returned a flat list of `DriverMetrics` with only
pool-level counters (`min`, `max`, `idle`, `used`, `host`). The new `Metrics.pools`
returns `PoolMetrics` objects enriched with context the pool manager already knows:

```python
@dataclass(frozen=True)
class PoolMetrics:
    host: str
    role: PoolRole | None          # PoolRole.MASTER | PoolRole.REPLICA | None
    healthy: bool
    min: int
    max: int
    idle: int
    used: int
    response_time: float | None    # health-check RTT
    in_flight: int                 # connections currently checked out
    staleness: PoolStaleness | None = None  # PoolStaleness.FRESH | .STALE
    lag: dict[str, Any] = {}       # staleness lag data (e.g. {"bytes": 1024})
    extra: dict[str, Any] = {}     # driver-specific data
```

**Before (0.9.0):**

```python
m = pool_manager.metrics()
for d in m.drivers:
    print(d.host, d.used)
```

**After (0.10.0):**

```python
m = pool_manager.metrics()
for p in m.pools:
    print(p.host, p.role, p.used, p.in_flight, p.response_time)
```

The old `m.drivers` property still works but emits a `DeprecationWarning`.

### 9. New `Metrics.gauges` field

`Metrics` now includes a `gauges: HasqlGauges` field — a point-in-time snapshot
of the pool manager state:

```python
@dataclass(frozen=True)
class HasqlGauges:
    master_count: int
    replica_count: int
    available_count: int
    active_connections: int
    closing: bool
    closed: bool
    stale_count: int = 0
    unavailable_count: int = 0
```

```python
m = pool_manager.metrics()
print(m.gauges.master_count, m.gauges.replica_count)
print(m.gauges.active_connections)
```

### 10. Driver-specific `extra` data

Drivers now surface rich introspection data via `PoolStats.extra`, which flows
through to `PoolMetrics.extra`:

- **psycopg3:** `pool_size`, `requests_waiting`, `requests_num`, `connections_errors`, `connections_lost`, etc.
- **SQLAlchemy:** `overflow`
- **aiopg / asyncpg:** empty (no extra data available)

---

## New capabilities

### Swappable drivers

With composition, you can now swap drivers without subclassing the manager:

```python
from hasql.driver.asyncpg import AsyncpgDriver
from hasql.pool_manager import BasePoolManager

class InstrumentedAsyncpgDriver(AsyncpgDriver):
    async def is_master(self, connection):
        start = time.monotonic()
        result = await super().is_master(connection)
        logger.info("is_master check took %.3fs", time.monotonic() - start)
        return result

pool = BasePoolManager(
    "postgresql://master,replica/db",
    driver=InstrumentedAsyncpgDriver(),
)
```

Drivers are also independently testable:

```python
async def test_my_driver():
    driver = MyDriver()
    pool = await driver.pool_factory(dsn, min_size=1, max_size=5)
    assert driver.get_pool_freesize(pool) == 5
    await driver.close_pool(pool)
```

### Pool state extraction

Pool state management has been extracted from `BasePoolManager` into a dedicated
`PoolState` class (`hasql/pool_state.py`). Raw state is deliberately not proxied
by the manager; this is an allowed breaking change in 0.10.0. See
[section 2](#2-manager-api-and-pool-state) for the supported manager API.
Custom orchestration should compose `PoolState` directly.

Balancer policies now depend on the `PoolStateProvider` protocol instead
of `BasePoolManager` directly. This breaks the circular import between
`pool_manager` and `balancer_policy`, and makes custom balancer policies
independently testable.

## Replica staleness configuration

```python
from datetime import timedelta

from hasql.driver.asyncpg import PoolManager
from hasql.staleness import BytesStalenessChecker, StalenessPolicy

pool = PoolManager(
    dsn,
    staleness=StalenessPolicy(
        BytesStalenessChecker(
            max_lag_bytes=16 * 1024 * 1024,
            max_master_lsn_age=timedelta(seconds=2),
        ),
        grace_period=timedelta(seconds=5),
    ),
)
```

The grace period is measured from a pool's last fresh result. A stale result
outside grace moves the pool out of the fresh replica set. With no fresh
replica, `fallback_master=True` prefers a master; otherwise a known stale
replica is used before waiting. Staleness-check exceptions fail closed and
clear cached policy state. Lag metrics use `lag["bytes"]` for byte checks and
`lag["time"]` (a `timedelta`) for time checks.
