# Migration Guide: hasql 0.9.0 → 0.10.0

## TL;DR

| What you do today | Action needed |
|---|---|
| `from hasql.aiopg import PoolManager` (or asyncpg, psycopg3, etc.) | **None** — works unchanged |
| `from hasql.base import BasePoolManager, TimeoutAcquireContext` | **None** — re-exports preserved |
| Subclass `BasePoolManager` to add a custom driver | **Rewrite** — extract driver into `PoolDriver` subclass |
| Override `_prepare_acquire_kwargs` | **Rewrite** — use explicit `timeout` parameter |
| Patch `_is_master` / `_pool_factory` in tests | **None** — proxy methods still patchable |
| Access `_refresh_role_tasks` | **Update** — use `_health.tasks` |
| Call `_notify_about_pool_has_checked` | **Update** — use `_health._notify_about_pool_has_checked` |

---

## What changed

### Architecture: Composition over Inheritance

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
| `hasql.balancer_policy` | `AbstractBalancerPolicy` |
| `hasql.pool_manager` | `BasePoolManager` (concrete) |
| `hasql.driver.*` | Driver implementations + PoolManager wrappers |

All old import paths from `hasql.base` continue to work via re-exports.

---

## No changes needed

### Using built-in PoolManagers

If you use hasql through the driver-specific `PoolManager` classes, nothing changes:

```python
# These all work exactly as before
from hasql.aiopg import PoolManager
from hasql.asyncpg import PoolManager
from hasql.psycopg3 import PoolManager
from hasql.asyncsqlalchemy import PoolManager
from hasql.aiopg_sa import PoolManager
from hasql.asyncpgsa import PoolManager

pool = PoolManager("postgresql://master,replica/db")
await pool.ready()

async with pool.acquire_master() as conn:
    ...
```

### Importing from `hasql.base`

All re-exports are preserved:

```python
# Still works
from hasql.base import (
    BasePoolManager,
    AbstractBalancerPolicy,
    TimeoutAcquireContext,
    PoolAcquireContext,
    AcquireContext,
    PoolT,
    ConnT,
    PoolDriver,  # NEW — also available here
)
```

### Patching driver methods in tests

Proxy methods on `BasePoolManager` still exist, so `mock.patch` continues to work:

```python
# Still works — _is_master is a proxy method on the manager
with mock.patch.object(pool_manager, "_is_master", ...):
    ...

# Also still works
with mock.patch("hasql.aiopg.PoolManager._is_master", ...):
    ...
```

---

## Migration required

### 1. Custom BasePoolManager subclasses

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

```python
# NEW — 0.10.0
from hasql.abc import PoolDriver
from hasql.acquire import TimeoutAcquireContext
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
        kwargs["min_size"] = kwargs.get("min_size", 1) + 1
        kwargs["max_size"] = kwargs.get("max_size", 10) + 1
        return kwargs

    async def close_pool(self, pool):  # was _close
        await pool.close()

    async def terminate_pool(self, pool):  # was _terminate
        pool.terminate()

    def is_connection_closed(self, connection):
        return connection.is_closed()

    def host(self, pool):
        return pool.host

    def driver_metrics(self, pools):  # was _driver_metrics(self)
        return [...]


class MyPoolManager(BasePoolManager[MyPool, MyConnection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=MyDriver(), **kwargs)
```

### Method name mapping

| Old (on BasePoolManager) | New (on PoolDriver) |
|---|---|
| `_is_master(connection)` | `is_master(connection)` |
| `_pool_factory(dsn)` | `pool_factory(dsn, **kwargs)` |
| `_prepare_pool_factory_kwargs(kwargs)` | `prepare_pool_factory_kwargs(kwargs)` |
| `_close(pool)` | `close_pool(pool)` |
| `_terminate(pool)` | `terminate_pool(pool)` |
| `_driver_metrics()` | `driver_metrics(pools)` |
| `get_pool_freesize(pool)` | `get_pool_freesize(pool)` |
| `acquire_from_pool(pool, **kwargs)` | `acquire_from_pool(pool, *, timeout=None, **kwargs)` |
| `release_to_pool(connection, pool)` | `release_to_pool(connection, pool)` |
| `is_connection_closed(connection)` | `is_connection_closed(connection)` |
| `host(pool)` | `host(pool)` |

Key differences:

- **Public names:** `_is_master` → `is_master`, `_close` → `close_pool`, etc.
- **`pool_factory` receives `**kwargs`:** The pool factory kwargs are passed
  as arguments instead of being read from `self.pool_factory_kwargs`.
- **`driver_metrics` receives `pools`:** Instead of reading `self.pools`
  from the manager, the pools sequence is passed as an argument.
- **`acquire_from_pool` has explicit `timeout`:** Timeout is a dedicated
  keyword argument, not smuggled through `**kwargs`.

### 2. `_prepare_acquire_kwargs` removed

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

### 3. `_refresh_role_tasks` → `_health.tasks`

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

### 4. `_notify_about_pool_has_checked` → `_health._notify_about_pool_has_checked`

**Before (0.9.0):**

```python
await self._notify_about_pool_has_checked(dsn)
```

**After (0.10.0):**

```python
await self._health._notify_about_pool_has_checked(dsn)
```

### 5. Accessing the driver instance

The driver is available via a property on the pool manager:

```python
pool = PoolManager("postgresql://master,replica/db")
driver = pool.driver  # PoolDriver instance
```

---

## New capability: Swappable drivers

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
