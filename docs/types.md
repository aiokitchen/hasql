# Type system in hasql

## Overview

hasql uses generic types to provide type-safe connection management across
multiple PostgreSQL drivers. The two core type variables flow through the
entire stack:

- **`PoolT`** — the driver-specific pool type (e.g. `asyncpg.Pool`,
  `AsyncConnectionPool`, `AsyncEngine`)
- **`ConnT`** — the driver-specific connection type (e.g. `asyncpg.Connection`,
  `psycopg.AsyncConnection`, `AsyncConnection`)

Each driver module binds these when subclassing `BasePoolManager`:

```python
# driver/asyncpg.py
class PoolManager(BasePoolManager[asyncpg.Pool, asyncpg.Connection]): ...

# driver/psycopg3.py
class PoolManager(BasePoolManager[AsyncConnectionPool, AsyncConnection]): ...

# driver/asyncsqlalchemy.py
class PoolManager(BasePoolManager[AsyncEngine, AsyncConnection]): ...

# driver/aiopg.py
class PoolManager(BasePoolManager[aiopg.Pool, aiopg.Connection]): ...
```

## Key protocols and generics

### AcquireContext[ConnT_co]

Structural protocol (`acquire.py`) expressing the dual async-context-manager +
awaitable pattern returned by `acquire_from_pool`:

```python
class AcquireContext(Protocol[ConnT_co]):
    async def __aenter__(self) -> ConnT_co: ...
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]: ...
    def __await__(self) -> Generator[Any, None, ConnT_co]: ...
```

`ConnT_co` is covariant — the protocol only produces connections, never
consumes them. This lets `asyncpg.PoolAcquireContext` (which returns
`PoolConnectionProxy`, a `Connection` subtype) satisfy
`AcquireContext[Connection]` without explicit registration.

### TimeoutAcquireContext[ConnT]

Generic wrapper (`acquire.py`) that decorates any `AcquireContext[ConnT]` with
an `asyncio.wait_for` deadline on both `__aenter__` and `__await__`.

### Stopwatch[KeyT]

Generic timer (`utils.py`) keyed by pool identity. `BasePoolManager` declares
`_stopwatch: Stopwatch[PoolT]` so response-time tracking is type-safe per pool.

### PoolAcquireContext[PoolT, ConnT]

The user-facing context manager (`acquire.py`) returned by
`pool_manager.acquire()`. Combines pool selection via the balancer with
connection acquisition from the chosen pool.

## Balancer policy typing

Balancer policies only care about pool selection — connection types are
irrelevant. They erase `ConnT` with `Any`:

```python
class AbstractBalancerPolicy(ABC, Generic[PoolT]):
    def __init__(self, pool_state: PoolStateProvider[PoolT]): ...
```

This is intentional: `GreedyBalancerPolicy[PoolT]` works with any
`PoolStateProvider` regardless of the connection type. Balancer policies
depend on the `PoolStateProvider` protocol (from `pool_state.py`), not
on `BasePoolManager` directly — this breaks the circular import.

## What was done

Refactored the codebase from untyped / `Any`-heavy signatures to proper
generics. Introduced `AcquireContext` Protocol, made `TimeoutAcquireContext`
and `Stopwatch` generic, added type annotations to all abstract methods in
`BasePoolManager`, and cleaned up suppression comments.

Removed 2 `# type: ignore` comments, narrowed 1, and replaced 5 `Any` usages
with proper generic types. Fixed a pre-existing type bug in `Stopwatch._cache`
(`Optional[int]` should have been `Optional[float]`).

## Remaining `# type: ignore` (5)

All justified — cannot be resolved without larger redesign or upstream changes.

| File | Line | Code | Reason |
|------|------|------|--------|
| `driver/asyncpg.py` | 4 | `import asyncpg  # type: ignore[import-untyped]` | No py.typed marker or stubs |
| `driver/asyncpgsa.py` | 1 | `import asyncpgsa  # type: ignore` | No type stubs at all |
| `driver/aiopg_sa.py` | 12 | `pools: ...  # type: ignore[assignment]` | Inherits `aiopg.PoolManager` (PoolT=Pool) but uses Engine |
| `driver/aiopg_sa.py` | 18 | `_pool_factory  # type: ignore[override]` | Return type differs from parent (Pool vs Engine) |
| `driver/aiopg_sa.py` | 24 | `host  # type: ignore[override]` | Parameter type differs from parent |

The three `aiopg_sa` ignores stem from a single root cause: `aiopg_sa.PoolManager`
inherits from `aiopg.PoolManager[aiopg.Pool, ...]` but actually works with
`aiopg.sa.Engine`. Fixing this requires rearchitecting the aiopg_sa inheritance
hierarchy.

## Remaining `Any` (10)

All justified — either unavoidable Python typing patterns or intentional erasure.

**`Generator[Any, None, T]` in `__await__`** (2 occurrences in `acquire.py`):
Standard Python typing. The yield type of `__await__` generators is always `Any`
per PEP 492 and typeshed conventions.

**`PoolStateProvider[PoolT]` in balancer policies** (in
`balancer_policy/base.py` and `balancer_policy/round_robin.py`):
Intentional ConnT erasure — balancers select pools, never touch connections.

**`Dsn.__init__(**kwargs: Any)`** and **`Dsn.__eq__(other: Any)`** in `utils.py`:
Standard Python patterns for arbitrary kwargs and equality comparison.

**`async_sessionmaker` parameters** (3 occurrences in `driver/asyncsqlalchemy.py`):
Pass-through to SQLAlchemy's `Session()` — mirrors upstream signatures.
