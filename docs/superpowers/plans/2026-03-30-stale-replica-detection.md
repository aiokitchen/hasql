# Stale Replica Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add pluggable staleness detection that removes lagging replicas from the active pool and falls back to them only as a last resort.

**Architecture:** New `hasql/staleness.py` module contains `CheckContext`, `StalenessCheckResult`, `BaseStalenessChecker`, `TimeStalenessChecker`, `BytesStalenessChecker`, and `StalenessPolicy`. `PoolState` gains `_stale_pool_set` and staleness checking. `BasePoolManager` accepts a `staleness` parameter. The balancer uses count-gated tiered fallback: fresh replicas > master > stale replicas > blocking wait.

**Tech Stack:** Python 3.10+, asyncio, pytest, `(str, Enum)` for Python 3.10 compat (no `StrEnum`)

**Spec:** `docs/superpowers/specs/2026-03-29-stale-replica-detection-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `hasql/staleness.py` | Create | `StalenessCheckResult`, `CheckContext`, `BaseStalenessChecker`, `TimeStalenessChecker`, `BytesStalenessChecker`, `StalenessPolicy` |
| `hasql/abc.py` | Modify | Add `fetch_scalar` to `PoolDriver` |
| `hasql/driver/asyncpg.py` | Modify | Implement `fetch_scalar` |
| `hasql/driver/aiopg.py` | Modify | Implement `fetch_scalar` |
| `hasql/driver/psycopg3.py` | Modify | Implement `fetch_scalar` |
| `hasql/driver/asyncsqlalchemy.py` | Modify | Implement `fetch_scalar` |
| `hasql/driver/aiopg_sa.py` | Modify | Implement `fetch_scalar` |
| `hasql/metrics.py` | Modify | Add `PoolRole`, `PoolStaleness` enums; update `PoolMetrics`, `HasqlGauges` |
| `hasql/pool_state.py` | Modify | Add `_stale_pool_set`, staleness methods, update `__iter__`, `available_pool_count`, `remove_pool_from_all_sets`, `clear_sets`, `PoolStateProvider` |
| `hasql/pool_manager.py` | Modify | Accept `staleness` param, add `_full_pool_check`, update `_periodic_pool_check`, update `metrics()` |
| `hasql/balancer_policy/base.py` | Modify | Update `_get_candidates` with tiered fallback |
| `tests/mocks/pool_manager.py` | Modify | Add `fetch_scalar` to `TestDriver` and `TestConnection` |
| `tests/test_staleness.py` | Create | Tests for staleness checkers and `StalenessPolicy` |
| `tests/test_staleness_integration.py` | Create | Integration tests: pool state transitions, balancer fallback, metrics |

---

### Task 1: Metrics Enums and Updated Dataclasses

**Files:**
- Modify: `hasql/metrics.py`
- Create: `tests/test_staleness.py`

- [x] **Step 1: Write failing test for PoolRole and PoolStaleness enums**

Create `tests/test_staleness.py`:

```python
from enum import Enum

from hasql.metrics import PoolRole, PoolStaleness


def test_pool_role_values():
    assert PoolRole.MASTER == "master"
    assert PoolRole.REPLICA == "replica"
    assert isinstance(PoolRole.MASTER, str)
    assert isinstance(PoolRole.MASTER, Enum)


def test_pool_staleness_values():
    assert PoolStaleness.FRESH == "fresh"
    assert PoolStaleness.STALE == "stale"
    assert isinstance(PoolStaleness.FRESH, str)
    assert isinstance(PoolStaleness.FRESH, Enum)


def test_pool_role_none_for_unavailable():
    """role=None means unavailable, backward compatible."""
    role: PoolRole | None = None
    assert role is None
    assert role != PoolRole.MASTER
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_staleness.py -v`
Expected: FAIL with `ImportError: cannot import name 'PoolRole'`

- [x] **Step 3: Add enums to metrics.py**

Add after the existing imports in `hasql/metrics.py`:

```python
from enum import Enum


class PoolRole(str, Enum):
    MASTER = "master"
    REPLICA = "replica"


class PoolStaleness(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
```

Update `__all__` to include `"PoolRole"` and `"PoolStaleness"`.

- [x] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_staleness.py -v`
Expected: PASS

- [x] **Step 5: Write failing test for updated PoolMetrics**

Add to `tests/test_staleness.py`:

```python
from hasql.metrics import PoolMetrics


def test_pool_metrics_with_staleness_fields():
    pm = PoolMetrics(
        host="localhost",
        role=PoolRole.REPLICA,
        healthy=True,
        min=1,
        max=10,
        idle=5,
        used=4,
        response_time=0.01,
        in_flight=1,
        staleness=PoolStaleness.FRESH,
        lag={"time": 1.5},
    )
    assert pm.staleness == PoolStaleness.FRESH
    assert pm.lag == {"time": 1.5}


def test_pool_metrics_staleness_none_for_master():
    pm = PoolMetrics(
        host="localhost",
        role=PoolRole.MASTER,
        healthy=True,
        min=1,
        max=10,
        idle=5,
        used=4,
        response_time=0.01,
        in_flight=0,
        staleness=None,
        lag={},
    )
    assert pm.staleness is None
    assert pm.lag == {}
```

- [x] **Step 6: Run test to verify it fails**

Run: `uv run pytest tests/test_staleness.py::test_pool_metrics_with_staleness_fields -v`
Expected: FAIL with `TypeError` (unexpected keyword `staleness`)

- [x] **Step 7: Add staleness and lag fields to PoolMetrics**

In `hasql/metrics.py`, update the `PoolMetrics` dataclass:

```python
@dataclass(frozen=True)
class PoolMetrics:
    """Per-pool metrics, enriched by the pool manager."""
    host: str
    role: str | None
    healthy: bool
    min: int
    max: int
    idle: int
    used: int
    response_time: float | None
    in_flight: int
    staleness: PoolStaleness | None = None
    lag: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)
```

Note: `staleness` and `lag` are added BEFORE `extra` but with defaults so existing callers using positional args for `extra` still work. All existing callers use keyword args.

- [x] **Step 8: Write failing test for updated HasqlGauges**

Add to `tests/test_staleness.py`:

```python
from hasql.metrics import HasqlGauges


def test_hasql_gauges_with_stale_and_unavailable():
    g = HasqlGauges(
        master_count=1,
        replica_count=2,
        available_count=4,
        active_connections=3,
        closing=False,
        closed=False,
        stale_count=1,
        unavailable_count=0,
    )
    assert g.stale_count == 1
    assert g.unavailable_count == 0
```

- [x] **Step 9: Run test to verify it fails**

Run: `uv run pytest tests/test_staleness.py::test_hasql_gauges_with_stale_and_unavailable -v`
Expected: FAIL with `TypeError` (unexpected keyword `stale_count`)

- [x] **Step 10: Add stale_count and unavailable_count to HasqlGauges**

In `hasql/metrics.py`, update `HasqlGauges`:

```python
@dataclass(frozen=True)
class HasqlGauges:
    """Point-in-time snapshot of pool manager state."""
    master_count: int
    replica_count: int
    available_count: int
    active_connections: int
    closing: bool
    closed: bool
    stale_count: int = 0
    unavailable_count: int = 0
```

- [x] **Step 11: Run all staleness tests**

Run: `uv run pytest tests/test_staleness.py -v`
Expected: all PASS

- [x] **Step 12: Run existing metrics tests to verify backward compat**

Run: `uv run pytest tests/test_metrics.py tests/test_calculate_metrics.py tests/test_backward_compat.py -v`
Expected: all PASS

- [x] **Step 13: Commit**

```bash
git add hasql/metrics.py tests/test_staleness.py
git commit -m "feat: add PoolRole, PoolStaleness enums and staleness fields to metrics"
```

---

### Task 2: StalenessCheckResult and CheckContext

**Files:**
- Create: `hasql/staleness.py`
- Modify: `tests/test_staleness.py`

- [x] **Step 1: Write failing test for StalenessCheckResult**

Add to `tests/test_staleness.py`:

```python
from hasql.staleness import StalenessCheckResult


def test_staleness_check_result_creation():
    result = StalenessCheckResult(is_stale=True, lag={"time": 5.0})
    assert result.is_stale is True
    assert result.lag == {"time": 5.0}


def test_staleness_check_result_is_frozen():
    result = StalenessCheckResult(is_stale=False, lag={})
    try:
        result.is_stale = True  # type: ignore[misc]
        assert False, "Should raise"
    except AttributeError:
        pass
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_staleness.py::test_staleness_check_result_creation -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'hasql.staleness'`

- [x] **Step 3: Create hasql/staleness.py with StalenessCheckResult**

Create `hasql/staleness.py`:

```python
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Any


@dataclass(frozen=True, slots=True)
class StalenessCheckResult:
    is_stale: bool
    lag: dict[str, Any]
```

- [x] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_staleness.py::test_staleness_check_result_creation tests/test_staleness.py::test_staleness_check_result_is_frozen -v`
Expected: PASS

- [x] **Step 5: Write failing test for CheckContext**

Add to `tests/test_staleness.py`:

```python
import pytest

from hasql.staleness import CheckContext


class MockDriver:
    async def fetch_scalar(self, connection, query):
        return f"result:{query}"


class MockConnection:
    pass


@pytest.mark.asyncio
async def test_check_context_fetch_scalar():
    driver = MockDriver()
    conn = MockConnection()
    ctx = CheckContext(connection=conn, driver=driver)
    result = await ctx.fetch_scalar("SELECT 1")
    assert result == "result:SELECT 1"
```

- [x] **Step 6: Run test to verify it fails**

Run: `uv run pytest tests/test_staleness.py::test_check_context_fetch_scalar -v`
Expected: FAIL with `ImportError: cannot import name 'CheckContext'`

- [x] **Step 7: Add CheckContext to hasql/staleness.py**

Add to `hasql/staleness.py`:

```python
class CheckContext:
    """Pre-bound query context for staleness checks.
    Created by PoolState with the current connection and driver.
    """
    __slots__ = ("_connection", "_driver")

    def __init__(self, connection: Any, driver: Any) -> None:
        self._connection = connection
        self._driver = driver

    async def fetch_scalar(self, query: str) -> Any:
        return await self._driver.fetch_scalar(self._connection, query)
```

- [x] **Step 8: Run test to verify it passes**

Run: `uv run pytest tests/test_staleness.py::test_check_context_fetch_scalar -v`
Expected: PASS

- [x] **Step 9: Commit**

```bash
git add hasql/staleness.py tests/test_staleness.py
git commit -m "feat: add StalenessCheckResult and CheckContext"
```

---

### Task 3: BaseStalenessChecker and TimeStalenessChecker

**Files:**
- Modify: `hasql/staleness.py`
- Modify: `tests/test_staleness.py`

- [x] **Step 1: Write failing test for BaseStalenessChecker interface**

Add to `tests/test_staleness.py`:

```python
from hasql.staleness import BaseStalenessChecker


@pytest.mark.asyncio
async def test_base_staleness_checker_collect_master_state_is_noop():
    """Default collect_master_state does nothing."""
    class NoopChecker(BaseStalenessChecker):
        async def check(self, ctx):
            return StalenessCheckResult(is_stale=False, lag={})

    checker = NoopChecker()
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())
    # Should not raise
    await checker.collect_master_state(ctx)
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_staleness.py::test_base_staleness_checker_collect_master_state_is_noop -v`
Expected: FAIL with `ImportError: cannot import name 'BaseStalenessChecker'`

- [x] **Step 3: Add BaseStalenessChecker to hasql/staleness.py**

Add to `hasql/staleness.py`:

```python
class BaseStalenessChecker(ABC):
    async def collect_master_state(self, ctx: CheckContext) -> None:
        pass

    @abstractmethod
    async def check(self, ctx: CheckContext) -> StalenessCheckResult: ...
```

- [x] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_staleness.py::test_base_staleness_checker_collect_master_state_is_noop -v`
Expected: PASS

- [x] **Step 5: Write failing tests for TimeStalenessChecker**

Add to `tests/test_staleness.py`:

```python
from datetime import timedelta

from hasql.staleness import TimeStalenessChecker


class TimeMockDriver:
    def __init__(self, lag_interval):
        self._lag_interval = lag_interval

    async def fetch_scalar(self, connection, query):
        return self._lag_interval


@pytest.mark.asyncio
async def test_time_staleness_checker_fresh():
    driver = TimeMockDriver(lag_interval=timedelta(seconds=5))
    ctx = CheckContext(connection=MockConnection(), driver=driver)
    checker = TimeStalenessChecker(max_lag=timedelta(seconds=10))
    result = await checker.check(ctx)
    assert result.is_stale is False
    assert result.lag == {"time": timedelta(seconds=5)}


@pytest.mark.asyncio
async def test_time_staleness_checker_stale():
    driver = TimeMockDriver(lag_interval=timedelta(seconds=15))
    ctx = CheckContext(connection=MockConnection(), driver=driver)
    checker = TimeStalenessChecker(max_lag=timedelta(seconds=10))
    result = await checker.check(ctx)
    assert result.is_stale is True
    assert result.lag == {"time": timedelta(seconds=15)}


@pytest.mark.asyncio
async def test_time_staleness_checker_null_replay_timestamp():
    driver = TimeMockDriver(lag_interval=None)
    ctx = CheckContext(connection=MockConnection(), driver=driver)
    checker = TimeStalenessChecker(max_lag=timedelta(seconds=10))
    result = await checker.check(ctx)
    assert result.is_stale is True
    assert result.lag == {}


@pytest.mark.asyncio
async def test_time_staleness_checker_exact_threshold():
    driver = TimeMockDriver(lag_interval=timedelta(seconds=10))
    ctx = CheckContext(connection=MockConnection(), driver=driver)
    checker = TimeStalenessChecker(max_lag=timedelta(seconds=10))
    result = await checker.check(ctx)
    assert result.is_stale is False
```

- [x] **Step 6: Run tests to verify they fail**

Run: `uv run pytest tests/test_staleness.py -k "time_staleness_checker" -v`
Expected: FAIL with `ImportError: cannot import name 'TimeStalenessChecker'`

- [x] **Step 7: Implement TimeStalenessChecker**

Add to `hasql/staleness.py`:

```python
class TimeStalenessChecker(BaseStalenessChecker):
    def __init__(self, max_lag: timedelta) -> None:
        self._max_lag = max_lag

    async def check(self, ctx: CheckContext) -> StalenessCheckResult:
        lag = await ctx.fetch_scalar(
            "SELECT clock_timestamp() - pg_last_xact_replay_timestamp()",
        )
        if lag is None:
            return StalenessCheckResult(is_stale=True, lag={})
        return StalenessCheckResult(
            is_stale=lag > self._max_lag,
            lag={"time": lag},
        )
```

- [x] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/test_staleness.py -k "time_staleness_checker" -v`
Expected: all PASS

- [x] **Step 9: Commit**

```bash
git add hasql/staleness.py tests/test_staleness.py
git commit -m "feat: add BaseStalenessChecker and TimeStalenessChecker"
```

---

### Task 4: BytesStalenessChecker

**Files:**
- Modify: `hasql/staleness.py`
- Modify: `tests/test_staleness.py`

- [x] **Step 1: Write failing tests for BytesStalenessChecker**

Add to `tests/test_staleness.py`:

```python
from hasql.staleness import BytesStalenessChecker


class BytesMockDriver:
    def __init__(self, lag_bytes=None, master_lsn=None):
        self._lag_bytes = lag_bytes
        self._master_lsn = master_lsn

    async def fetch_scalar(self, connection, query):
        if "pg_current_wal_lsn" in query:
            return self._master_lsn
        if "pg_wal_lsn_diff" in query:
            return self._lag_bytes
        return None


@pytest.mark.asyncio
async def test_bytes_staleness_checker_fresh():
    driver = BytesMockDriver(lag_bytes=1000)
    ctx_master = CheckContext(connection=MockConnection(), driver=BytesMockDriver(master_lsn="0/1000000"))
    ctx_replica = CheckContext(connection=MockConnection(), driver=driver)

    checker = BytesStalenessChecker(max_lag_bytes=1024 * 1024)
    await checker.collect_master_state(ctx_master)
    result = await checker.check(ctx_replica)
    assert result.is_stale is False
    assert result.lag == {"bytes": 1000}


@pytest.mark.asyncio
async def test_bytes_staleness_checker_stale():
    driver = BytesMockDriver(lag_bytes=2 * 1024 * 1024)
    ctx_master = CheckContext(connection=MockConnection(), driver=BytesMockDriver(master_lsn="0/2000000"))
    ctx_replica = CheckContext(connection=MockConnection(), driver=driver)

    checker = BytesStalenessChecker(max_lag_bytes=1024 * 1024)
    await checker.collect_master_state(ctx_master)
    result = await checker.check(ctx_replica)
    assert result.is_stale is True
    assert result.lag == {"bytes": 2 * 1024 * 1024}


@pytest.mark.asyncio
async def test_bytes_staleness_checker_no_master_lsn():
    driver = BytesMockDriver(lag_bytes=999)
    ctx = CheckContext(connection=MockConnection(), driver=driver)

    checker = BytesStalenessChecker(max_lag_bytes=100)
    result = await checker.check(ctx)
    assert result.is_stale is False
    assert result.lag == {}


@pytest.mark.asyncio
async def test_bytes_staleness_checker_stale_master_lsn(monkeypatch):
    """If cached master LSN is too old, assume fresh."""
    driver = BytesMockDriver(lag_bytes=2 * 1024 * 1024)
    ctx_master = CheckContext(connection=MockConnection(), driver=BytesMockDriver(master_lsn="0/3000000"))
    ctx_replica = CheckContext(connection=MockConnection(), driver=driver)

    checker = BytesStalenessChecker(
        max_lag_bytes=1024 * 1024,
        max_master_lsn_age=timedelta(seconds=2),
    )
    await checker.collect_master_state(ctx_master)

    # Simulate time passing beyond max_master_lsn_age
    checker._master_lsn_updated_at -= 3.0

    result = await checker.check(ctx_replica)
    assert result.is_stale is False
    assert result.lag == {}
```

- [x] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_staleness.py -k "bytes_staleness_checker" -v`
Expected: FAIL with `ImportError: cannot import name 'BytesStalenessChecker'`

- [x] **Step 3: Implement BytesStalenessChecker**

Add to `hasql/staleness.py`:

```python
class BytesStalenessChecker(BaseStalenessChecker):
    def __init__(
        self,
        max_lag_bytes: int,
        max_master_lsn_age: timedelta = timedelta(seconds=2),
    ) -> None:
        self._max_lag_bytes = max_lag_bytes
        self._max_master_lsn_age = max_master_lsn_age
        self._master_lsn: str | None = None
        self._master_lsn_updated_at: float | None = None

    async def collect_master_state(self, ctx: CheckContext) -> None:
        self._master_lsn = await ctx.fetch_scalar(
            "SELECT pg_current_wal_lsn()",
        )
        self._master_lsn_updated_at = time.monotonic()

    async def check(self, ctx: CheckContext) -> StalenessCheckResult:
        if self._master_lsn is None or self._master_lsn_updated_at is None:
            return StalenessCheckResult(is_stale=False, lag={})

        age = time.monotonic() - self._master_lsn_updated_at
        if age > self._max_master_lsn_age.total_seconds():
            return StalenessCheckResult(is_stale=False, lag={})

        lag_bytes = await ctx.fetch_scalar(
            f"SELECT pg_wal_lsn_diff('{self._master_lsn}'::pg_lsn,"
            f" pg_last_wal_replay_lsn())::bigint",
        )
        return StalenessCheckResult(
            is_stale=lag_bytes > self._max_lag_bytes,
            lag={"bytes": lag_bytes},
        )
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_staleness.py -k "bytes_staleness_checker" -v`
Expected: all PASS

- [x] **Step 5: Commit**

```bash
git add hasql/staleness.py tests/test_staleness.py
git commit -m "feat: add BytesStalenessChecker"
```

---

### Task 5: StalenessPolicy

**Files:**
- Modify: `hasql/staleness.py`
- Modify: `tests/test_staleness.py`

- [x] **Step 1: Write failing tests for StalenessPolicy**

Add to `tests/test_staleness.py`:

```python
from hasql.staleness import StalenessPolicy


class AlwaysFreshChecker(BaseStalenessChecker):
    async def check(self, ctx):
        return StalenessCheckResult(is_stale=False, lag={"time": timedelta(seconds=1)})


class AlwaysStaleChecker(BaseStalenessChecker):
    async def check(self, ctx):
        return StalenessCheckResult(is_stale=True, lag={"time": timedelta(seconds=20)})


@pytest.mark.asyncio
async def test_staleness_policy_fresh():
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())
    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is False


@pytest.mark.asyncio
async def test_staleness_policy_stale():
    policy = StalenessPolicy(checker=AlwaysStaleChecker())
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())
    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is True


@pytest.mark.asyncio
async def test_staleness_policy_grace_period():
    """Pool stays fresh during grace period even when checker says stale."""
    policy = StalenessPolicy(
        checker=AlwaysFreshChecker(),
        grace_period=timedelta(seconds=30),
    )
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())

    # First check: fresh — records last_fresh_at
    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is False

    # Switch to stale checker
    policy._checker = AlwaysStaleChecker()

    # Second check: checker says stale but within grace period
    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is False
    assert result.lag == {"time": timedelta(seconds=20)}


@pytest.mark.asyncio
async def test_staleness_policy_grace_period_expired():
    """Pool becomes stale after grace period expires."""
    policy = StalenessPolicy(
        checker=AlwaysStaleChecker(),
        grace_period=timedelta(seconds=1),
    )
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())

    # Manually set last_fresh_at in the past
    policy._last_fresh_at["pool1"] = time.monotonic() - 2.0

    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is True


@pytest.mark.asyncio
async def test_staleness_policy_remove_pool():
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())

    await policy.check(pool="pool1", ctx=ctx)
    assert "pool1" in policy._last_fresh_at

    policy.remove_pool("pool1")
    assert "pool1" not in policy._last_fresh_at


@pytest.mark.asyncio
async def test_staleness_policy_remove_pool_not_tracked():
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    # Should not raise
    policy.remove_pool("nonexistent")
```

- [x] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_staleness.py -k "staleness_policy" -v`
Expected: FAIL with `ImportError: cannot import name 'StalenessPolicy'`

- [x] **Step 3: Implement StalenessPolicy**

Add to `hasql/staleness.py`:

```python
class StalenessPolicy:
    """User-facing configuration for staleness detection.
    Non-generic — pool identity is opaque (used only as dict keys).
    """

    def __init__(
        self,
        checker: BaseStalenessChecker,
        grace_period: timedelta | None = None,
    ) -> None:
        self._checker = checker
        self._grace_period = grace_period
        self._last_fresh_at: dict[Any, float] = {}

    async def check(
        self,
        pool: Any,
        ctx: CheckContext,
    ) -> StalenessCheckResult:
        result = await self._checker.check(ctx)
        if not result.is_stale:
            self._last_fresh_at[pool] = time.monotonic()
            return result
        if self._grace_period is not None and pool in self._last_fresh_at:
            age = time.monotonic() - self._last_fresh_at[pool]
            if age < self._grace_period.total_seconds():
                return StalenessCheckResult(is_stale=False, lag=result.lag)
        return result

    async def collect_master_state(self, ctx: CheckContext) -> None:
        await self._checker.collect_master_state(ctx)

    def remove_pool(self, pool: Any) -> None:
        self._last_fresh_at.pop(pool, None)
```

Update `__all__` in `hasql/staleness.py` (add at end of file):

```python
__all__ = (
    "StalenessCheckResult",
    "CheckContext",
    "BaseStalenessChecker",
    "TimeStalenessChecker",
    "BytesStalenessChecker",
    "StalenessPolicy",
)
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_staleness.py -k "staleness_policy" -v`
Expected: all PASS

- [x] **Step 5: Run all staleness tests**

Run: `uv run pytest tests/test_staleness.py -v`
Expected: all PASS

- [x] **Step 6: Commit**

```bash
git add hasql/staleness.py tests/test_staleness.py
git commit -m "feat: add StalenessPolicy with grace period support"
```

---

### Task 6: PoolDriver.fetch_scalar and Driver Implementations

**Files:**
- Modify: `hasql/abc.py`
- Modify: `hasql/driver/asyncpg.py`
- Modify: `hasql/driver/aiopg.py`
- Modify: `hasql/driver/psycopg3.py`
- Modify: `hasql/driver/asyncsqlalchemy.py`
- Modify: `hasql/driver/aiopg_sa.py`
- Modify: `tests/mocks/pool_manager.py`

- [x] **Step 1: Add fetch_scalar to PoolDriver ABC**

In `hasql/abc.py`, add after `is_master`:

```python
    @abstractmethod
    async def fetch_scalar(self, connection: ConnT, query: str) -> Any:
        """Execute a query and return a single scalar value."""
        ...
```

Add `Any` to imports: `from typing import Any, Generic, TypeVar`

- [x] **Step 2: Implement fetch_scalar in all drivers**

In `hasql/driver/asyncpg.py`, add to `AsyncpgDriver`:

```python
    async def fetch_scalar(self, connection, query):
        return await connection.fetchval(query)
```

In `hasql/driver/aiopg.py`, add to `AiopgDriver`:

```python
    async def fetch_scalar(self, connection, query):
        cursor = await connection.cursor()
        async with cursor:
            await cursor.execute(query)
            row = await cursor.fetchone()
            return row[0] if row else None
```

In `hasql/driver/psycopg3.py`, add to `Psycopg3Driver`:

```python
    async def fetch_scalar(self, connection: AsyncConnection, query: str):
        async with connection.cursor() as cur:
            await cur.execute(query)
            row = await cur.fetchone()
            if row is None:
                return None
            return row[0]
```

In `hasql/driver/asyncsqlalchemy.py`, add to `AsyncSqlAlchemyDriver`:

```python
    async def fetch_scalar(self, connection: AsyncConnection, query: str):
        result = await connection.scalar(sa.text(query))
        await connection.execute(sa.text("COMMIT"))
        return result
```

In `hasql/driver/aiopg_sa.py`, add to `AiopgSaDriver`:

```python
    async def fetch_scalar(self, connection: aiopg.sa.SAConnection, query: str):
        return await connection.scalar(query)
```

- [x] **Step 3: Update TestDriver in mocks**

In `tests/mocks/pool_manager.py`, add to `TestConnection`:

```python
    async def fetch_scalar(self, query):
        if not self._pool.is_running:
            raise ConnectionRefusedError
        return self._pool.fetch_scalar_result
```

Add to `TestPool.__init__`:

```python
        self.fetch_scalar_result = None
```

Add to `TestDriver`:

```python
    async def fetch_scalar(self, connection: TestConnection, query: str):
        return await connection.fetch_scalar(query)
```

- [x] **Step 4: Run lint to verify**

Run: `uv run ruff check hasql/abc.py hasql/driver/ tests/mocks/`
Expected: no errors

- [x] **Step 5: Commit**

```bash
git add hasql/abc.py hasql/driver/ tests/mocks/pool_manager.py
git commit -m "feat: add fetch_scalar to PoolDriver and all driver implementations"
```

---

### Task 7: PoolState Staleness Support

**Files:**
- Modify: `hasql/pool_state.py`
- Create: `tests/test_staleness_integration.py`

- [x] **Step 1: Write failing tests for stale pool set management**

Create `tests/test_staleness_integration.py`:

```python
import asyncio
from datetime import timedelta

import pytest

from tests.mocks.pool_manager import TestPoolManager


@pytest.fixture
def make_dsn():
    def factory(replicas=2):
        hosts = ",".join(
            [f"master"] + [f"replica{i}" for i in range(replicas)],
        )
        return f"postgresql://test:test@{hosts}:5432/test"
    return factory


@pytest.fixture
async def pool_manager(make_dsn):
    pm = TestPoolManager(make_dsn(replicas=2))
    await pm.ready(masters_count=1, replicas_count=2)
    yield pm
    await pm.close()


async def test_pool_state_stale_pool_set_empty_by_default(pool_manager):
    ps = pool_manager._pool_state
    assert ps.stale_pool_count == 0
    assert ps.get_stale_pools() == []


async def test_pool_state_stale_pools_in_iter(pool_manager):
    ps = pool_manager._pool_state
    all_pools = list(ps)
    # master + 2 replicas, no stale
    assert len(all_pools) == 3


async def test_pool_state_available_pool_count_includes_stale(pool_manager):
    ps = pool_manager._pool_state
    assert ps.available_pool_count == 3  # 1 master + 2 replicas + 0 stale
```

- [x] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_staleness_integration.py -v`
Expected: FAIL with `AttributeError: 'PoolState' object has no attribute 'stale_pool_count'`

- [x] **Step 3: Add stale pool set to PoolState**

In `hasql/pool_state.py`, modify `__init__` to add after `_replica_pool_set`:

```python
        self._stale_pool_set: set[PoolT] = set()
```

Add `stale_pool_count` property after `replica_pool_count`:

```python
    @property
    def stale_pool_count(self) -> int:
        return len(self._stale_pool_set)
```

Update `available_pool_count`:

```python
    @property
    def available_pool_count(self) -> int:
        return self.master_pool_count + self.replica_pool_count + self.stale_pool_count
```

Add `get_stale_pools` method in the "Pool retrieval" section:

```python
    def get_stale_pools(self) -> list[PoolT]:
        return list(self._stale_pool_set)
```

Update `remove_pool_from_all_sets`:

```python
    def remove_pool_from_all_sets(self, pool: PoolT, dsn: Dsn):
        self._remove_pool_from_master_set(pool, dsn)
        self._remove_pool_from_replica_set(pool, dsn)
        self._stale_pool_set.discard(pool)
```

Update `clear_sets`:

```python
    def clear_sets(self):
        self._master_pool_set.clear()
        self._replica_pool_set.clear()
        self._stale_pool_set.clear()
```

Update `__iter__`:

```python
    def __iter__(self):
        return chain(
            iter(self._master_pool_set),
            iter(self._replica_pool_set),
            iter(self._stale_pool_set),
        )
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_staleness_integration.py -v`
Expected: all PASS

- [x] **Step 5: Add staleness support to PoolState**

Add imports at top of `hasql/pool_state.py`:

```python
from .staleness import CheckContext, StalenessCheckResult, StalenessPolicy
```

Modify `PoolState.__init__` to accept `staleness` parameter — add after the `_stopwatch` line:

```python
        self._staleness: StalenessPolicy | None = staleness
        self._last_check_result: dict[PoolT, StalenessCheckResult] = {}
```

Update `__init__` signature to add `staleness: StalenessPolicy | None = None`:

```python
    def __init__(
        self,
        dsn_list: list[Dsn],
        driver: PoolDriver[PoolT, ConnT],
        stopwatch_window_size: int,
        pool_factory_kwargs: dict | None = None,
        staleness: StalenessPolicy | None = None,
    ):
```

Add staleness methods after `refresh_pool_role`:

```python
    def _make_check_context(self, connection: ConnT) -> CheckContext:
        return CheckContext(connection, self._driver)

    async def check_replica_staleness(
        self, pool: PoolT, dsn: Dsn, connection: ConnT,
    ):
        if not self._staleness:
            return
        if pool not in self._replica_pool_set and pool not in self._stale_pool_set:
            return
        ctx = self._make_check_context(connection)
        result = await self._staleness.check(pool, ctx)
        self._last_check_result[pool] = result
        if result.is_stale:
            self._replica_pool_set.discard(pool)
            self._stale_pool_set.add(pool)
            logger.info(
                "Pool %s marked as stale",
                dsn.with_(password="******"),
            )
        else:
            self._stale_pool_set.discard(pool)
            if pool not in self._replica_pool_set:
                self._replica_pool_set.add(pool)
                logger.info(
                    "Pool %s recovered from stale",
                    dsn.with_(password="******"),
                )

    async def collect_master_state(self, connection: ConnT):
        if self._staleness:
            ctx = self._make_check_context(connection)
            await self._staleness.collect_master_state(ctx)
```

Update `remove_pool_from_all_sets` to clean up staleness state:

```python
    def remove_pool_from_all_sets(self, pool: PoolT, dsn: Dsn):
        self._remove_pool_from_master_set(pool, dsn)
        self._remove_pool_from_replica_set(pool, dsn)
        self._stale_pool_set.discard(pool)
        self._last_check_result.pop(pool, None)
        if self._staleness:
            self._staleness.remove_pool(pool)
```

Update `clear_sets`:

```python
    def clear_sets(self):
        self._master_pool_set.clear()
        self._replica_pool_set.clear()
        self._stale_pool_set.clear()
        self._last_check_result.clear()
```

Update `PoolStateProvider` protocol — add after `get_last_response_time`:

```python
    @property
    def replica_pool_count(self) -> int: ...
    @property
    def stale_pool_count(self) -> int: ...
    def get_stale_pools(self) -> list[PoolT]: ...
```

- [x] **Step 6: Run all tests**

Run: `uv run pytest tests/test_staleness_integration.py tests/test_staleness.py -v`
Expected: all PASS

- [x] **Step 7: Run existing tests to check for regressions**

Run: `uv run pytest tests/test_base_pool_manager.py tests/test_balancer_policy.py -v`
Expected: all PASS

- [x] **Step 8: Commit**

```bash
git add hasql/pool_state.py tests/test_staleness_integration.py
git commit -m "feat: add stale pool set and staleness checking to PoolState"
```

---

### Task 8: BasePoolManager Integration

**Files:**
- Modify: `hasql/pool_manager.py`
- Modify: `tests/mocks/pool_manager.py`

- [x] **Step 1: Update BasePoolManager to accept staleness parameter**

In `hasql/pool_manager.py`, add import:

```python
from .staleness import StalenessPolicy
```

Update `__init__` signature — add `staleness` parameter:

```python
    def __init__(
        self,
        dsn: str,
        *,
        driver: PoolDriver[PoolT, ConnT],
        acquire_timeout: float = DEFAULT_ACQUIRE_TIMEOUT,
        refresh_delay: float = DEFAULT_REFRESH_DELAY,
        refresh_timeout: float = DEFAULT_REFRESH_TIMEOUT,
        fallback_master: bool = False,
        master_as_replica_weight: float = DEFAULT_MASTER_AS_REPLICA_WEIGHT,
        balancer_policy: type[AbstractBalancerPolicy] = GreedyBalancerPolicy,
        stopwatch_window_size: int = DEFAULT_STOPWATCH_WINDOW_SIZE,
        pool_factory_kwargs: dict | None = None,
        staleness: StalenessPolicy | None = None,
    ):
```

Pass `staleness` to `PoolState`:

```python
        self._pool_state: PoolState[PoolT, ConnT] = PoolState(
            dsn_list=split_dsn(dsn),
            driver=driver,
            stopwatch_window_size=stopwatch_window_size,
            pool_factory_kwargs=pool_factory_kwargs,
            staleness=staleness,
        )
```

- [x] **Step 2: Update _periodic_pool_check with staleness hooks**

Replace `_periodic_pool_check` with:

```python
    async def _periodic_pool_check(
        self,
        pool: PoolT,
        dsn: Dsn,
        sys_connection: ConnT,
    ):
        while not self._closing:
            try:
                await asyncio.wait_for(
                    self._full_pool_check(pool, dsn, sys_connection),
                    timeout=self._refresh_timeout,
                )
                await self._pool_state.notify_pool_checked(dsn)
            except asyncio.TimeoutError:
                logger.warning(
                    "Periodic pool check failed for dsn=%s",
                    dsn.with_(password="******"),
                )
                self._pool_state.remove_pool_from_all_sets(pool, dsn)
                await self._pool_state.notify_pool_checked(dsn)

            await asyncio.sleep(self._refresh_delay)

    async def _full_pool_check(
        self,
        pool: PoolT,
        dsn: Dsn,
        sys_connection: ConnT,
    ):
        await self._pool_state.refresh_pool_role(pool, dsn, sys_connection)
        if self._pool_state.pool_is_master(pool):
            await self._pool_state.collect_master_state(sys_connection)
        else:
            await self._pool_state.check_replica_staleness(
                pool, dsn, sys_connection,
            )
```

- [x] **Step 3: Update metrics() to include staleness fields**

Replace the `metrics()` method:

```python
    def metrics(self) -> Metrics:
        pool_state = self._pool_state
        pool_metrics = []
        for pool in pool_state.pools:
            if pool is None:
                continue
            stats = pool_state.pool_stats(pool)

            if pool_state.pool_is_master(pool):
                role = "master"
            elif pool_state.pool_is_replica(pool):
                role = "replica"
            else:
                role = None

            staleness = None
            lag: dict[str, Any] = {}
            if role == "replica":
                check_result = pool_state.get_last_check_result(pool)
                if check_result is not None:
                    staleness = (
                        PoolStaleness.FRESH
                        if not check_result.is_stale
                        else PoolStaleness.STALE
                    )
                    lag = check_result.lag
                else:
                    staleness = PoolStaleness.FRESH
            elif pool_state.pool_is_stale(pool):
                role = "replica"
                check_result = pool_state.get_last_check_result(pool)
                staleness = PoolStaleness.STALE
                if check_result is not None:
                    lag = check_result.lag

            in_flight = sum(
                1 for p in self._unmanaged_connections.values() if p is pool
            )

            pool_metrics.append(PoolMetrics(
                host=pool_state.host(pool),
                role=role,
                healthy=role is not None,
                min=stats.min,
                max=stats.max,
                idle=stats.idle,
                used=stats.used,
                response_time=pool_state.get_last_response_time(pool),
                in_flight=in_flight,
                staleness=staleness,
                lag=lag,
                extra=stats.extra,
            ))

        gauges = HasqlGauges(
            master_count=pool_state.master_pool_count,
            replica_count=pool_state.replica_pool_count,
            available_count=pool_state.available_pool_count,
            active_connections=len(self._unmanaged_connections),
            closing=self._closing,
            closed=self._closed,
            stale_count=pool_state.stale_pool_count,
            unavailable_count=(
                len([p for p in pool_state.pools if p is not None])
                - pool_state.available_pool_count
            ),
        )

        return Metrics(
            pools=pool_metrics,
            hasql=self._metrics.metrics(),
            gauges=gauges,
        )
```

Add imports at top:

```python
from .metrics import CalculateMetrics, HasqlGauges, Metrics, PoolMetrics, PoolStaleness
```

Add `pool_is_stale` and `get_last_check_result` to `PoolState` (in `hasql/pool_state.py`):

```python
    def pool_is_stale(self, pool: PoolT) -> bool:
        return pool in self._stale_pool_set

    def get_last_check_result(self, pool: PoolT) -> StalenessCheckResult | None:
        return self._last_check_result.get(pool)
```

- [x] **Step 4: Update TestPoolManager in mocks**

In `tests/mocks/pool_manager.py`, update `TestPoolManager`:

```python
class TestPoolManager(BasePoolManager[TestPool, TestConnection]):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, driver=TestDriver(), **kwargs)
```

No changes needed — `staleness` is already an optional kwarg passed through `**kwargs`.

- [x] **Step 5: Run existing tests to check for regressions**

Run: `uv run pytest tests/ -v --timeout=30`
Expected: all PASS

- [x] **Step 6: Commit**

```bash
git add hasql/pool_manager.py hasql/pool_state.py tests/mocks/pool_manager.py
git commit -m "feat: integrate staleness into BasePoolManager and health check loop"
```

---

### Task 9: Balancer Tiered Fallback

**Files:**
- Modify: `hasql/balancer_policy/base.py`
- Modify: `tests/test_staleness_integration.py`

- [x] **Step 1: Write failing tests for tiered fallback**

Add to `tests/test_staleness_integration.py`:

```python
from hasql.staleness import (
    BaseStalenessChecker,
    CheckContext,
    StalenessCheckResult,
    StalenessPolicy,
    TimeStalenessChecker,
)


class ConfigurableChecker(BaseStalenessChecker):
    """Checker that returns stale for specific pools."""
    def __init__(self):
        self.stale_pools: set[str] = set()

    async def check(self, ctx):
        # Use a marker on the context to identify which pool
        return StalenessCheckResult(
            is_stale=True,
            lag={"time": timedelta(seconds=99)},
        )


async def test_balancer_prefers_fresh_over_stale():
    """When fresh replicas exist, stale replicas are not selected."""
    dsn = "postgresql://test:test@master,replica0,replica1:5432/test"
    pm = TestPoolManager(dsn)
    await pm.ready(masters_count=1, replicas_count=2)

    ps = pm._pool_state
    # Move one replica to stale set manually
    replica_pools = list(ps._replica_pool_set)
    stale_pool = replica_pools[0]
    fresh_pool = replica_pools[1]
    ps._replica_pool_set.discard(stale_pool)
    ps._stale_pool_set.add(stale_pool)

    # Acquire should get the fresh pool
    async with pm.acquire_replica() as conn:
        assert conn._pool is fresh_pool

    await pm.close()


async def test_balancer_falls_back_to_stale():
    """When no fresh replicas or masters, falls back to stale."""
    dsn = "postgresql://test:test@master,replica0:5432/test"
    pm = TestPoolManager(dsn)
    await pm.ready(masters_count=1, replicas_count=1)

    ps = pm._pool_state
    # Move all replicas to stale
    replica_pools = list(ps._replica_pool_set)
    for pool in replica_pools:
        ps._replica_pool_set.discard(pool)
        ps._stale_pool_set.add(pool)

    # Also remove master to test stale-only fallback
    master_pools = list(ps._master_pool_set)
    for pool in master_pools:
        ps._master_pool_set.discard(pool)

    # Should fall back to stale
    async with pm.acquire_replica() as conn:
        assert conn._pool in ps._stale_pool_set

    await pm.close()
```

- [x] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_staleness_integration.py::test_balancer_prefers_fresh_over_stale -v`
Expected: FAIL (balancer doesn't know about stale pools yet)

- [x] **Step 3: Update _get_candidates in balancer base**

In `hasql/balancer_policy/base.py`, replace `_get_candidates`:

```python
    async def _get_candidates(
        self,
        read_only: bool,
        fallback_master: bool = False,
        choose_master_as_replica: bool = False,
    ) -> list[PoolT]:
        candidates: list[PoolT] = []

        if read_only:
            if self._pool_state.replica_pool_count > 0:
                candidates.extend(
                    await self._pool_state.get_replica_pools(
                        fallback_master=False,
                    ),
                )
            elif fallback_master and self._pool_state.master_pool_count > 0:
                candidates.extend(
                    await self._pool_state.get_master_pools(),
                )
            elif self._pool_state.stale_pool_count > 0:
                candidates.extend(self._pool_state.get_stale_pools())
            else:
                candidates.extend(
                    await self._pool_state.get_replica_pools(
                        fallback_master=fallback_master,
                    ),
                )

        if not read_only or (
            choose_master_as_replica
            and self._pool_state.master_pool_count > 0
        ):
            candidates.extend(await self._pool_state.get_master_pools())

        return candidates
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_staleness_integration.py -v`
Expected: all PASS

- [x] **Step 5: Run full balancer test suite**

Run: `uv run pytest tests/test_balancer_policy.py -v`
Expected: all PASS

- [x] **Step 6: Commit**

```bash
git add hasql/balancer_policy/base.py tests/test_staleness_integration.py
git commit -m "feat: add tiered fallback to balancer (fresh > master > stale > wait)"
```

---

### Task 10: Lint, Type Check, Full Test Suite

**Files:** None (verification only)

- [x] **Step 1: Run ruff**

Run: `uv run ruff check hasql/ tests/`
Expected: no errors (fix any issues found)

- [x] **Step 2: Run mypy**

Run: `uv run mypy hasql/`
Expected: no new errors

- [x] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v --timeout=30`
Expected: all tests PASS

- [x] **Step 4: Commit any fixes**

```bash
git add -u
git commit -m "fix: lint and type checking fixes for staleness feature"
```
