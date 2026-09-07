# Timeout handling in hasql

## Overview

hasql uses a **timeout budget** approach. A single `acquire_timeout` value
(set per-manager or per-call) is shared across two phases:

1. **Pool selection** — waiting for a healthy pool via the balancer.
2. **Connection acquisition** — getting a connection from the selected pool.

The remaining budget after pool selection is forwarded to the driver adapter,
which enforces it using the driver's native mechanism.

## Manager-level timeout

```python
PoolManager(dsn, acquire_timeout=2.0, ...)
```

`acquire_timeout` (default `1.0` s) is the total time budget for both phases.
It can be overridden per call:

```python
pool_manager.acquire(timeout=5.0)
pool_manager.acquire_master(timeout=5.0)
pool_manager.acquire_replica(timeout=5.0)
```

Setting `timeout=None` (the default) uses the manager-level `acquire_timeout`.

## Per-driver behavior

Each driver adapter receives the remaining timeout budget and enforces it
using the most appropriate mechanism:

### asyncpg

`AsyncpgDriver.acquire_from_pool(timeout=<remaining>)` passes the budget to
`pool.acquire(timeout=...)`. asyncpg raises `asyncio.TimeoutError` natively.

### psycopg3

`Psycopg3Driver.acquire_from_pool(timeout=<remaining>)` passes the budget to
`pool.getconn(timeout=...)`. psycopg3 raises `psycopg_pool.PoolTimeout`
natively.

> **Breaking (0.9.0):** Previous versions forced `max_waiting=-1` (unlimited
> queue) on psycopg3 pools. This override has been removed. If you relied on
> unlimited waiting, pass `pool_factory_kwargs={"max_waiting": -1}` explicitly.

### aiopg / aiopg_sa

aiopg's `pool.acquire()` does not accept a timeout parameter. hasql wraps
the acquire call with `asyncio.wait_for(timeout=<remaining>)` inside the
driver adapter. Raises `asyncio.TimeoutError` on timeout.

### SQLAlchemy async (asyncsqlalchemy)

`pool.connect()` does not accept a timeout parameter. hasql wraps
the connect call with `asyncio.wait_for(timeout=<remaining>)` inside the
driver adapter. Raises `asyncio.TimeoutError` on timeout.

## Background health-check timeouts

These are separate from the acquire timeout:

| Parameter         | Default | Purpose                                       |
|-------------------|---------|-----------------------------------------------|
| `refresh_delay`   | 1 s     | Interval between health checks                |
| `refresh_timeout` | 30 s    | Timeout for a single health-check iteration   |

Health checks acquire a system connection and run
`SHOW transaction_read_only`, followed by configured staleness collection or
checking. If the role check exceeds `refresh_timeout`, the existing timeout
path removes the pool from availability until a later successful check. Any
exception from staleness collection/checking also fails closed: the pool is
removed from master, replica, and stale sets and its cached lag/grace tracking
is cleared.

## Timeout flow diagram

```
acquire(timeout=T)
  |
  +-- deadline = now + T
  |
  +-- _get_pool(deadline)                   [pool selection, uses remaining budget]
  |     waits up to (deadline - now)
  |
  +-- remaining = deadline - now
  |
  +-- pool_state.acquire_from_pool(timeout=remaining)
        delegates to the driver, which enforces its native timeout
```
