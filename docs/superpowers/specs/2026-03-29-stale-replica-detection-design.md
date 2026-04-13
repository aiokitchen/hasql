# Stale Replica Detection

## Problem

hasql currently detects whether a PostgreSQL host is a master or replica (`SHOW transaction_read_only`), but has no mechanism to detect **replication lag**. A replica that is reachable but significantly behind the master continues to receive read traffic, potentially serving stale data.

## Solution

Add a pluggable staleness detection system that removes lagging replicas from the active replica pool and falls back to them only when no better options are available.

## Fallback Priority

When acquiring a read-only connection, the balancer uses this priority:

1. **Fresh replicas** — replicas that pass the staleness check
2. **Master** — only if `fallback_master=True`
3. **Stale replicas** — last resort, only if no fresh replicas and no masters available

When staleness checking is disabled (default), behavior is identical to current — no changes to the fallback chain.

## Pool States

Two orthogonal dimensions tracked per pool:

```python
# (str, Enum) instead of StrEnum for Python 3.10 compatibility
class PoolRole(str, Enum):
    MASTER = "master"
    REPLICA = "replica"
    # No UNAVAILABLE — unavailable pools keep role=None, same as current behavior.

class PoolStaleness(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
```

`PoolRole | None` — `None` means unavailable/unknown (backward compatible with current `role is None` checks and OTLP `"unknown"` label).

`PoolStaleness` only applies to replicas. For master and unavailable pools, the `staleness` field in `PoolMetrics` is `None`.

## Staleness Checker Hierarchy

New file: `hasql/staleness.py`

### CheckContext

Wraps a connection and driver into a single query interface. Checkers never see the raw connection — they only interact through this context:

```python
class CheckContext:
    """Pre-bound query context for staleness checks.
    Created by PoolState with the current connection and driver.
    """
    async def fetch_scalar(self, query: str) -> Any:
        """Execute a query and return a single scalar value."""
        ...
```

Extensible — `fetch_row` or similar methods can be added later without changing checker signatures.

### BaseStalenessChecker

```python
@dataclass(frozen=True, slots=True)
class StalenessCheckResult:
    is_stale: bool
    lag: dict[str, Any]  # checker-specific lag metrics

class BaseStalenessChecker(ABC):
    async def collect_master_state(self, ctx: CheckContext) -> None:
        """Hook called during master's health check cycle.
        Override to cache master-side data (e.g., WAL LSN).
        No-op by default.
        """
        pass

    @abstractmethod
    async def check(self, ctx: CheckContext) -> StalenessCheckResult:
        """Called during replica's health check cycle.
        Returns staleness verdict and lag metrics in a single query.
        """
        ...
```

`BaseStalenessChecker` is not generic — the connection is hidden behind `CheckContext`. The `check()` method returns both the staleness verdict and lag metrics in one call, avoiding duplicate queries.

### Built-in Implementations

#### TimeStalenessChecker

Uses `pg_last_xact_replay_timestamp()` on the replica. No master query needed.

**Warning: false positives during idle periods.** `pg_last_xact_replay_timestamp()` only advances when the replica replays a transaction. During periods of no write activity on master, the replay timestamp stops advancing even though the replica is fully caught up. For bursty workloads this means replicas get marked stale during quiet periods, funneling all reads to the master — the opposite of what you want. Use `grace_period` to mitigate this, or prefer `BytesStalenessChecker` for bursty workloads.

```python
class TimeStalenessChecker(BaseStalenessChecker):
    def __init__(self, max_lag: timedelta):
        self._max_lag = max_lag

    async def check(self, ctx: CheckContext) -> StalenessCheckResult:
        # Use clock_timestamp() instead of now() — now() returns transaction-start time
        # and can freeze on connections with implicit open transactions.
        # lag = await ctx.fetch_scalar("SELECT clock_timestamp() - pg_last_xact_replay_timestamp()")
        # If lag is NULL (no replay yet): return StalenessCheckResult(is_stale=True, lag={})
        # return StalenessCheckResult(
        #     is_stale=lag > self._max_lag,
        #     lag={"time": lag},
        # )
```

`grace_period` is configured on `StalenessPolicy`, not on the checker (see StalenessPolicy section). It prevents flapping during idle periods when `pg_last_xact_replay_timestamp()` stops advancing due to no writes.

#### BytesStalenessChecker

Compares replica WAL replay position against master's current WAL position.

```python
class BytesStalenessChecker(BaseStalenessChecker):
    def __init__(
        self,
        max_lag_bytes: int,
        max_master_lsn_age: timedelta = timedelta(seconds=2),
    ):
        self._max_lag_bytes = max_lag_bytes
        # Default 2s matches default refresh_delay=1s. Adjust if using non-default refresh_delay.
        self._max_master_lsn_age = max_master_lsn_age
        self._master_lsn: str | None = None  # pg_lsn stored as string
        self._master_lsn_updated_at: float | None = None  # time.monotonic()

    async def collect_master_state(self, ctx: CheckContext) -> None:
        # PostgreSQL returns pg_lsn type, not int. Use pg_wal_lsn_diff for byte distance.
        # self._master_lsn = await ctx.fetch_scalar("SELECT pg_current_wal_lsn()")
        # self._master_lsn_updated_at = time.monotonic()

    async def check(self, ctx: CheckContext) -> StalenessCheckResult:
        # If self._master_lsn is None: return StalenessCheckResult(is_stale=False, lag={})
        # If master LSN is too old (age > max_master_lsn_age): return StalenessCheckResult(is_stale=False, lag={})
        #
        # Avoid parameterized query — interpolate cached LSN directly (it's a
        # trusted internal value from pg_current_wal_lsn(), not user input):
        # lag_bytes = await ctx.fetch_scalar(
        #     f"SELECT pg_wal_lsn_diff('{self._master_lsn}'::pg_lsn, pg_last_wal_replay_lsn())::bigint",
        # )
        # return StalenessCheckResult(
        #     is_stale=lag_bytes > self._max_lag_bytes,
        #     lag={"bytes": lag_bytes},
        # )
```

**Race condition mitigation:** master and replica health checks run in independent loops. The cached `_master_lsn` can be arbitrarily old by the time a replica reads it. To prevent false positives/negatives:
- `_master_lsn_updated_at` tracks when the LSN was cached
- If the cached LSN is older than `max_master_lsn_age` (default: `2s`), `check()` returns `is_stale=False` (unknown = assume fresh)
- Accuracy of `BytesStalenessChecker` is bounded by the health check interval

### Driver Query Support

Staleness checkers are driver-agnostic. They interact with the database only through `CheckContext`, which delegates to the driver.

A new method is added to `PoolDriver`:

```python
class PoolDriver:
    async def fetch_scalar(self, connection: ConnT, query: str) -> Any:
        """Execute a query and return a single scalar value."""
```

Each driver implements this using its native API (asyncpg `fetchval`, aiopg `cursor.execute` + `fetchone`, etc.). No parameterized query support — built-in checkers use static SQL or f-string interpolation with trusted internal values only. Custom checkers that need parameterized queries must build SQL strings themselves.

`PoolState` creates a `CheckContext` by binding the current connection and driver, then passes it to the checker. The checker never sees the raw connection or driver.

## StalenessPolicy

Owns the checker, grace period, and per-pool freshness tracking. Does not own the stale pool set (that stays in `PoolState` alongside master/replica sets to avoid duplicating set management).

Passed as a single `staleness` argument to `BasePoolManager`.

```python
class StalenessPolicy:
    """User-facing configuration for staleness detection.
    Non-generic — pool identity is opaque (used only as dict keys).
    """
    def __init__(
        self,
        checker: BaseStalenessChecker,
        grace_period: timedelta | None = None,
    ):
        self._checker = checker
        self._grace_period = grace_period
        self._last_fresh_at: dict[Any, float] = {}

    async def check(self, pool: Any, ctx: CheckContext) -> StalenessCheckResult:
        """Returns staleness verdict (with grace period) and lag metrics in one call."""
        result = await self._checker.check(ctx)
        if not result.is_stale:
            self._last_fresh_at[pool] = time.monotonic()
            return result
        if self._grace_period and pool in self._last_fresh_at:
            age = time.monotonic() - self._last_fresh_at[pool]
            if age < self._grace_period.total_seconds():
                return StalenessCheckResult(is_stale=False, lag=result.lag)
        return result

    async def collect_master_state(self, ctx: CheckContext) -> None:
        await self._checker.collect_master_state(ctx)

    def remove_pool(self, pool: Any) -> None:
        """Clean up tracking when pool is removed from all sets."""
        self._last_fresh_at.pop(pool, None)
```

## PoolState Changes

`PoolState` gains `_stale_pool_set` and `_staleness`.

**Invariant: a pool entering `_replica_pool_set` is presumed fresh until proven otherwise.** When `refresh_pool_role()` adds a pool to `_replica_pool_set`, it is immediately eligible for read traffic. The staleness check runs as a separate step in the same loop iteration, but there is a brief window between the two calls where the balancer could hand out a connection from a replica that hasn't been staleness-checked yet. This applies both to initial pool discovery (startup) and to pools re-entering the replica set after recovery. This is intentional — a replica that just appeared is more useful than no replica, and the staleness check within the same iteration will demote it if it's actually stale.

```python
class PoolState:
    _master_pool_set: set[PoolT]
    _replica_pool_set: set[PoolT]
    _stale_pool_set: set[PoolT]              # new
    _staleness: StalenessPolicy | None         # new, None = feature disabled
    _last_check_result: dict[PoolT, StalenessCheckResult]  # new, cached for metrics

    @property
    def stale_pool_count(self) -> int:
        return len(self._stale_pool_set)

    @property
    def available_pool_count(self) -> int:
        # Updated: stale replicas are available (they have a known role)
        return self.master_pool_count + self.replica_pool_count + self.stale_pool_count

    def _make_check_context(self, connection) -> CheckContext:
        """Bind connection + driver into a CheckContext."""
        return CheckContext(connection, self._driver)

    async def check_replica_staleness(self, pool, dsn, connection):
        """Called after refresh_pool_role(). Only acts on replicas."""
        if not self._staleness:
            return
        if pool not in self._replica_pool_set and pool not in self._stale_pool_set:
            return
        ctx = self._make_check_context(connection)
        result = await self._staleness.check(pool, ctx)
        self._last_check_result[pool] = result  # cached for metrics
        if result.is_stale:
            self._replica_pool_set.discard(pool)
            self._stale_pool_set.add(pool)
            # log: pool marked as stale
        else:
            self._stale_pool_set.discard(pool)
            self._replica_pool_set.add(pool)
            # log: pool recovered from stale

    async def collect_master_state(self, connection):
        """Called after refresh_pool_role() confirms master."""
        if self._staleness:
            ctx = self._make_check_context(connection)
            await self._staleness.collect_master_state(ctx)

    def remove_pool_from_all_sets(self, pool, dsn):
        # existing: remove from master + replica
        # new: also remove from _stale_pool_set
        self._last_check_result.pop(pool, None)
        if self._staleness:
            self._staleness.remove_pool(pool)

    def get_stale_pools(self) -> list[PoolT]:
        return list(self._stale_pool_set)

    def __iter__(self):
        # Updated: include stale pools (they are open pools with connections)
        return chain(
            iter(self._master_pool_set),
            iter(self._replica_pool_set),
            iter(self._stale_pool_set),
        )
```

**Return type consistency:** `get_stale_pools()`, `get_replica_pools()`, and `get_master_pools()` all return `list[PoolT]`. Callers should not need `list(...)` wrapping.

## Health Check Loop Changes

`_periodic_pool_check` in `pool_manager.py` adds two calls after `refresh_pool_role`:

**Timeout coverage:** The existing code wraps only `refresh_pool_role()` in `asyncio.wait_for`. Staleness queries (`collect_master_state`, `check_replica_staleness`) must also be covered — a hanging `fetch_scalar()` would stall the health task instead of evicting the pool. The timeout must wrap the entire health iteration:

```python
async def _periodic_pool_check(self, pool, dsn, sys_connection):
    while not self._closing:
        try:
            await asyncio.wait_for(
                self._full_pool_check(pool, dsn, sys_connection),
                timeout=self._refresh_timeout,
            )
            await self._pool_state.notify_pool_checked(dsn)
        except asyncio.TimeoutError:
            logger.warning(...)
            self._pool_state.remove_pool_from_all_sets(pool, dsn)
            await self._pool_state.notify_pool_checked(dsn)

        await asyncio.sleep(self._refresh_delay)

async def _full_pool_check(self, pool, dsn, sys_connection):
    await self._pool_state.refresh_pool_role(pool, dsn, sys_connection)

    if self._pool_state.pool_is_master(pool):
        await self._pool_state.collect_master_state(sys_connection)
    else:
        await self._pool_state.check_replica_staleness(pool, dsn, sys_connection)
```

Staleness check failures (non-timeout exceptions) are caught by existing exception handling in `health.py`, which removes the pool from all sets.

## Balancer Changes

**Blocking wait problem:** The existing `get_replica_pools()` blocks (via `wait_for_replica_pools()`) until at least one replica is available. The tiered fallback chain cannot call blocking `get_replica_pools(fallback_master=False)` when no fresh replicas exist — it would hang instead of falling through to master/stale tiers.

**Solution:** Use count-gated non-blocking selection across tiers, with a blocking fallback when ALL tiers are empty. This preserves the existing wait-for-recovery semantics: if no pools exist at all, the acquire flow blocks on the original `get_replica_pools()` getter, bounded by `acquire_timeout`.

A new `stale_pool_count` property is needed on `PoolState`.

```python
async def _get_candidates(self, read_only, fallback_master, choose_master_as_replica):
    candidates = []

    if read_only:
        # Tier selection: non-blocking when pools exist in any tier
        if self._pool_state.replica_pool_count > 0:
            # Tier 1: fresh replicas
            candidates = await self._pool_state.get_replica_pools(fallback_master=False)
        elif fallback_master and self._pool_state.master_pool_count > 0:
            # Tier 2: master (if fallback_master)
            candidates = await self._pool_state.get_master_pools()
        elif self._pool_state.stale_pool_count > 0:
            # Tier 3: stale replicas
            candidates = self._pool_state.get_stale_pools()
        else:
            # All tiers empty — block on original getter, preserving wait-for-recovery.
            # Outer asyncio.wait_for(acquire_timeout) provides the deadline.
            candidates = await self._pool_state.get_replica_pools(
                fallback_master=fallback_master,
            )

    if not read_only or choose_master_as_replica:
        candidates.extend(await self._pool_state.get_master_pools())

    return candidates
```

**Wait semantics preserved:**
- Write path (`read_only=False`): `get_master_pools()` still blocks waiting for master recovery — unchanged.
- Read path, all tiers empty: falls through to blocking `get_replica_pools(fallback_master)` — same as current behavior, bounded by `acquire_timeout`.
- Read path, only stale available: immediately uses stale replicas — no unnecessary waiting.
- TOCTOU race (count > 0 then set empties before getter returns): the blocking getter waits, bounded by `acquire_timeout`.

No changes needed in `greedy.py`, `round_robin.py`, or `random_weighted.py`.

When staleness is disabled, `stale_pool_count` is always 0 and `get_stale_pools()` returns empty — the `else` branch triggers, falling through to the original blocking getter. Behavior identical to current.

## Metrics

Lag metrics are part of `StalenessCheckResult.lag`, returned by `BaseStalenessChecker.check()`. No separate `get_lag()` method needed — one query produces both the staleness verdict and lag data. `PoolState` caches the result in `_last_check_result[pool]` for metrics consumption.

```python
class PoolMetrics:
    role: PoolRole | None           # master / replica / None (unavailable)
    staleness: PoolStaleness | None # fresh / stale for replicas, None for master / unavailable
    lag: dict[str, Any]             # from StalenessCheckResult.lag, empty dict if no checker
    healthy: bool                   # see backward compatibility below
    # ... existing fields unchanged

class HasqlGauges:
    master_count: int               # existing
    replica_count: int              # existing
    available_count: int            # existing — see backward compatibility below
    stale_count: int                # new
    unavailable_count: int          # new
    # ... existing fields unchanged
```

Built-in keys are documented: `"time"` (`timedelta`) for `TimeStalenessChecker`, `"bytes"` (`int`) for `BytesStalenessChecker`. Custom checkers define their own keys.

### Backward Compatibility

Existing metrics semantics must be preserved:

- **`PoolMetrics.role`**: Type changes from `str | None` to `PoolRole | None`. Since `PoolRole` is `(str, Enum)`, string comparisons like `role == "master"` still work. `None` still means unavailable/unknown — no change. Code checking `role is None` continues to work.
- **`PoolMetrics.healthy`**: Unchanged. Still `role is not None`. Stale replicas have `role = PoolRole.REPLICA` (not `None`), so `healthy` remains `True`. Stale ≠ unhealthy.
- **`HasqlGauges.available_count`**: Currently "total pools with a known role." Stale replicas have a known role, so they count as available. `available_count = master_count + replica_count + stale_count`.
- **`HasqlGauges.replica_count`**: Only counts **fresh** replicas (pools in `_replica_pool_set`). Stale replicas are counted separately in `stale_count`. This is a semantic change — document it in the changelog.
- **OTLP labels**: The `role` label value for stale replicas remains `"replica"` (from `PoolRole.REPLICA`). The new `staleness` label distinguishes fresh from stale. Existing dashboards filtering on `role="replica"` will not see stale replicas disappear — they just get an additional label.

## Public API

### Constructor

```python
# Time-based with grace period:
PoolManager(
    dsn="postgresql://db1,db2,db3/mydb",
    staleness=StalenessPolicy(
        checker=TimeStalenessChecker(max_lag=timedelta(seconds=10)),
        grace_period=timedelta(seconds=30),
    ),
)

# Bytes-based:
PoolManager(
    dsn="postgresql://db1,db2,db3/mydb",
    staleness=StalenessPolicy(
        checker=BytesStalenessChecker(max_lag_bytes=16 * 1024 * 1024),
    ),
)

# Disabled (default — backward compatible):
PoolManager(dsn="postgresql://db1,db2,db3/mydb")
```

`BasePoolManager.__init__` accepts `staleness: StalenessPolicy | None = None`. Driver-specific pool managers forward the kwarg. The `StalenessPolicy` instance must not be shared across multiple pool managers — each manager should receive its own instance (the policy tracks per-pool state internally).

### No changes to acquire API

`acquire_replica()` / `acquire()` signatures are unchanged. Staleness is fully internal — stale replicas are excluded from the fresh replica set and only used as a last-resort fallback.

## Edge Cases

| Scenario | Behavior |
|---|---|
| No master available | `BytesStalenessChecker.collect_master_state` never called, `_master_lsn` stays `None`, `check()` returns `is_stale=False` (assume fresh) |
| Master LSN is stale (timing drift, GC pause) | `BytesStalenessChecker` checks `_master_lsn_updated_at` age against `max_master_lsn_age`. If too old, returns `False` (assume fresh) to avoid false positives/negatives |
| No write activity on master | `TimeStalenessChecker` may falsely mark replicas as stale (replay timestamp stops advancing). `BytesStalenessChecker` is unaffected. |
| All replicas stale, master available | `fallback_master=True`: uses master. `fallback_master=False`: uses stale replicas. |
| All replicas stale, no master | Uses stale replicas. |
| Staleness check query fails | Existing health check error handling removes pool from all sets. |
| Replica recovers from lag | Next successful health check where `check()` returns `is_stale=False` moves it back to `_replica_pool_set`. |
| Synchronous replicas | Synchronous replicas cannot lag behind master beyond the configured synchronous commit level. Staleness checking adds overhead with no benefit in this case — do not configure `staleness` for synchronous replication setups. |

## File Changes Summary

| File | Change |
|---|---|
| `hasql/staleness.py` | **New.** `StalenessCheckResult`, `CheckContext`, `BaseStalenessChecker`, `TimeStalenessChecker`, `BytesStalenessChecker`, `StalenessPolicy` |
| `hasql/abc.py` | Add `fetch_scalar` to `PoolDriver` |
| `hasql/driver/*.py` | Implement `fetch_scalar` in each driver |
| `hasql/pool_state.py` | Add `_stale_pool_set`, `_staleness`, `_last_check_result`, `check_replica_staleness()`, `collect_master_state()`, `get_stale_pools()`, `stale_pool_count`. Update `remove_pool_from_all_sets()`, `available_pool_count`, `__iter__`. |
| `hasql/pool_manager.py` | Accept `staleness` param, wrap timeout around full health iteration (`_full_pool_check`). Update metrics building for new fields. |
| `hasql/balancer_policy/base.py` | Update `_get_candidates()` with count-gated tiered fallback (fresh replicas > master > stale). |
| `hasql/metrics.py` | Add `PoolRole` (`str, Enum`, master/replica only), `PoolStaleness` (`str, Enum`). Add `staleness`, `lag` to `PoolMetrics`. Add `stale_count`, `unavailable_count` to `HasqlGauges`. `role` stays `PoolRole | None` (None = unavailable, backward compatible). |
| `tests/` | Tests for staleness checkers, pool state stale transitions, balancer fallback order, metrics backward compatibility. |
