# Metrics Improvement Plan

## Problem Statement

The current metrics system has correctness bugs, a rigid data model that discards
driver-specific information, and a responsibility boundary that prevents the pool
manager from enriching per-pool metrics with the context it already owns (role,
health, response time).

### Recent Architecture Change: `PoolState` / `PoolStateProvider`

Pool state has been extracted from `BasePoolManager` into a dedicated `PoolState`
class (in `hasql/pool_state.py`). The manager now composes it as the public
`self.pool_state` attribute. Key consequences for the metrics plan:

- **Master/replica sets** live at `pool_state._master_pool_set` /
  `pool_state._replica_pool_set`
- **Pools list** lives at `pool_state._pools`
- **Stopwatch** lives at `pool_state._stopwatch`
- **Role queries** (`pool_is_master`, `pool_is_replica`) live on `PoolState`
- **Freesize** is available via `pool_state.get_pool_freesize(pool)`
- **Response time** is available via `pool_state.get_last_response_time(pool)`
- **Balancer policies** depend on the `PoolStateProvider` protocol, not
  `BasePoolManager`
- **`BasePoolManager`** remains the owner of `_unmanaged_connections`, `_metrics`
  (`CalculateMetrics`), `_driver`, `_closing`, `_closed`, and the public
  `metrics()` entry point

The metrics enrichment in Phase 3 must read from **both** `self.pool_state` (role,
health, stopwatch) and `self` (unmanaged connections, driver, lifecycle flags).

---

## Phase 1 — Fix Bugs in Current Code ✅ DONE

> No API changes. Pure bugfixes. Can be released as a patch.

### 1.1 Fix psycopg3 `used` computation ✅

**File:** `hasql/driver/psycopg3.py`
**Bug:** `used=stat["pool_size"]` reports total pool size, not active connections.

```python
# BEFORE (wrong)
used=stat["pool_size"],

# AFTER (correct)
used=stat["pool_size"] - stat["pool_available"],
```

psycopg3's `pool_size` is the number of connections currently managed by the pool
(idle + checked-out + being prepared). The correct active count is
`pool_size - pool_available`, which matches every other driver's pattern
(`size - freesize`).

**Test fix:** `tests/test_psycopg3.py` — change expected `used=11` to `used=2`.

### 1.2 Fix asyncpg `used` computation ✅

**File:** `hasql/driver/asyncpg.py`
**Bug:** `used=p._maxsize - self.get_pool_freesize(p)` uses `_maxsize` instead of
actual current size. If the pool hasn't grown to maxsize yet, this over-reports.

```python
# BEFORE (inaccurate when pool hasn't grown to maxsize)
used=p._maxsize - self.get_pool_freesize(p),

# AFTER
used=p.get_size() - self.get_pool_freesize(p),
```

`asyncpg.Pool.get_size()` returns the actual number of connections currently in the
pool. This is the public API equivalent of `len(p._holders)`.

**Risk:** Low. `get_size()` has been available since asyncpg 0.10. In practice most
pools have `min_size == max_size` so the values were already the same, but the fix
makes it correct for dynamic-size pools.

---

## Phase 2 — New Driver ABC Method: `pool_stats` ✅ DONE

> Replace `driver_metrics()` with a simpler `pool_stats()` that returns raw data
> for a **single pool**. The manager will handle iteration, None-filtering, and
> enrichment.

### 2.1 Add `PoolStats` dataclass ✅

**File:** `hasql/metrics.py`

```python
@dataclass(frozen=True)
class PoolStats:
    """Raw pool statistics returned by a driver for a single pool."""
    min: int
    max: int
    idle: int
    used: int
    extra: Dict[str, Any] = field(default_factory=dict)
```

`extra` is an opaque bag for driver-specific data. Drivers that have richer
introspection (psycopg3, SQLAlchemy) put it here. Drivers that don't just leave it
empty.

### 2.2 Add `pool_stats()` to `PoolDriver` ABC ✅

**File:** `hasql/abc.py`

```python
@abstractmethod
def pool_stats(self, pool: PoolT) -> PoolStats: ...
```

This replaces `driver_metrics(pools: Sequence[Optional[PoolT]])`. The new method
operates on a single pool (never None), keeping the driver's job minimal.

### 2.3 Implement `pool_stats()` in all 5 drivers ✅

Each driver already has the logic; it just moves from a list comprehension into a
per-pool method. All per-driver `driver_metrics()` overrides removed.

**aiopg:**
```python
def pool_stats(self, pool):
    return PoolStats(
        min=pool.minsize,
        max=pool.maxsize or 0,
        idle=pool.freesize,
        used=pool.size - pool.freesize,
    )
```

**aiopg_sa:**
```python
def pool_stats(self, pool):
    return PoolStats(
        min=pool.minsize,
        max=pool.maxsize,
        idle=pool.freesize,
        used=pool.size - pool.freesize,
    )
```

**asyncpg:**
```python
def pool_stats(self, pool):
    idle = self.get_pool_freesize(pool)
    return PoolStats(
        min=pool._minsize,
        max=pool._maxsize,
        idle=idle,
        used=pool.get_size() - idle,
    )
```

**psycopg3** (surfaces all `get_stats()` keys via `extra`):
```python
def pool_stats(self, pool):
    stats = pool.get_stats()
    return PoolStats(
        min=stats["pool_min"],
        max=stats["pool_max"],
        idle=stats["pool_available"],
        used=stats["pool_size"] - stats["pool_available"],
        extra={
            "pool_size": stats["pool_size"],
            "requests_waiting": stats.get("requests_waiting", 0),
            "requests_num": stats.get("requests_num", 0),
            "requests_queued": stats.get("requests_queued", 0),
            "requests_wait_ms": stats.get("requests_wait_ms", 0),
            "requests_errors": stats.get("requests_errors", 0),
            "returns_bad": stats.get("returns_bad", 0),
            "connections_num": stats.get("connections_num", 0),
            "connections_ms": stats.get("connections_ms", 0),
            "connections_errors": stats.get("connections_errors", 0),
            "connections_lost": stats.get("connections_lost", 0),
            "usage_ms": stats.get("usage_ms", 0),
        },
    )
```

**asyncsqlalchemy:**
```python
def pool_stats(self, pool):
    qp = pool.sync_engine.pool
    return PoolStats(
        min=0,
        max=qp.size(),
        idle=qp.checkedin(),
        used=qp.checkedout(),
        extra={
            "overflow": qp.overflow(),
        },
    )
```

### 2.4 Deprecate `driver_metrics()` on `PoolDriver` ✅

`driver_metrics()` is no longer abstract. It has a default implementation that
delegates to `pool_stats()` and emits a `DeprecationWarning`:

```python
# In PoolDriver ABC — no longer abstract, has a default:
def driver_metrics(self, pools: Sequence[Optional[PoolT]]) -> Sequence[DriverMetrics]:
    warnings.warn(
        "driver_metrics() is deprecated, implement pool_stats() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return [
        DriverMetrics(
            min=s.min, max=s.max, idle=s.idle, used=s.used,
            host=self.host(p),
        )
        for p in pools if p
        for s in [self.pool_stats(p)]
    ]
```

Per-driver `driver_metrics()` overrides removed from all 5 drivers. Mock
`TestDriver` updated with `pool_stats()` returning real pool state.

---

## Phase 3 — New `PoolMetrics` and Manager-Side Enrichment ✅ DONE

> The manager builds per-pool metrics that combine driver stats with role, health,
> response time, and in-flight connection count. Data comes from two sources:
> `self.pool_state` (role, pools list, stopwatch) and `self` (driver, unmanaged
> connections).

### 3.1 Add `PoolMetrics` dataclass

**File:** `hasql/metrics.py`

```python
@dataclass(frozen=True)
class PoolMetrics:
    """Per-pool metrics, enriched by the pool manager."""
    # Identity
    host: str
    role: Optional[str]           # "master" | "replica" | None (unhealthy/unknown)
    healthy: bool                  # True if pool is in master_pool_set or replica_pool_set

    # Standard pool gauges
    min: int
    max: int
    idle: int
    used: int

    # Hasql-level per-pool data
    response_time: Optional[float]  # median health-check RTT from Stopwatch
    in_flight: int                  # connections currently checked out via this pool

    # Driver-specific extras
    extra: Dict[str, Any]
```

### 3.2 Rewrite `BasePoolManager.metrics()`

**File:** `hasql/pool_manager.py`

The manager now builds `PoolMetrics` by combining driver stats with pool state.
Note: after the `PoolState` refactor, role/health data lives on `self.pool_state`
while `_unmanaged_connections` and `_driver` remain on `self`.

```python
def metrics(self) -> Metrics:
    pool_state = self.pool_state
    pool_metrics = []
    for pool in pool_state._pools:
        if pool is None:
            continue
        stats = self._driver.pool_stats(pool)

        if pool_state.pool_is_master(pool):
            role = "master"
        elif pool_state.pool_is_replica(pool):
            role = "replica"
        else:
            role = None

        in_flight = sum(
            1 for p in self._unmanaged_connections.values() if p is pool
        )

        pool_metrics.append(PoolMetrics(
            host=self._driver.host(pool),
            role=role,
            healthy=role is not None,
            min=stats.min,
            max=stats.max,
            idle=stats.idle,
            used=stats.used,
            response_time=pool_state.get_last_response_time(pool),
            in_flight=in_flight,
            extra=stats.extra,
        ))

    return Metrics(
        pools=pool_metrics,
        hasql=self._metrics.metrics(),
    )
```

**Key difference from the old plan:** All pool-state queries go through
`self.pool_state` (the `PoolState` instance), not through private attributes on
`self`. The methods `pool_is_master()`, `pool_is_replica()`, and
`get_last_response_time()` are public on `PoolState`. The `_pools` list is accessed
via `pool_state._pools` (consistent with how `health.py` already accesses it).

### 3.3 Update the `Metrics` dataclass

**File:** `hasql/metrics.py`

```python
@dataclass(frozen=True)
class Metrics:
    pools: Sequence[PoolMetrics]
    hasql: HasqlMetrics

    @property
    def drivers(self) -> Sequence[DriverMetrics]:
        """Backward-compatible accessor. Deprecated."""
        warnings.warn(
            "Metrics.drivers is deprecated, use Metrics.pools instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return [
            DriverMetrics(
                min=p.min, max=p.max, idle=p.idle, used=p.used, host=p.host,
            )
            for p in self.pools
        ]
```

### 3.4 Remove `_driver_metrics()` proxy from `BasePoolManager`

The `_driver_metrics()` method on `BasePoolManager` (which currently calls
`self._driver.driver_metrics(self.pool_state._pools)`) is no longer needed — the
new `metrics()` calls `self._driver.pool_stats()` directly per pool. Remove the
proxy method.

---

## Phase 4 — Extend `HasqlMetrics` with Gauges ✅ DONE

> Add a real-time snapshot section to complement the existing cumulative counters.

### 4.1 Add `HasqlGauges` dataclass

**File:** `hasql/metrics.py`

```python
@dataclass(frozen=True)
class HasqlGauges:
    """Point-in-time snapshot of pool manager state."""
    master_count: int
    replica_count: int
    available_count: int
    active_connections: int   # len(_unmanaged_connections)
    closing: bool
    closed: bool
```

### 4.2 Add `gauges` as a top-level field on `Metrics`

We add `gauges` as a sibling to `pools` and `hasql` on `Metrics`, rather than
nesting it inside `HasqlMetrics`. This avoids changing the `HasqlMetrics` frozen
dataclass shape for existing consumers who destructure it, and keeps the conceptual
separation clear (point-in-time snapshot vs cumulative counters).

```python
@dataclass(frozen=True)
class Metrics:
    pools: Sequence[PoolMetrics]
    hasql: HasqlMetrics
    gauges: HasqlGauges

    @property
    def drivers(self) -> Sequence[DriverMetrics]:
        """Backward-compatible accessor. Deprecated."""
        ...
```

### 4.3 Build gauges in `BasePoolManager.metrics()`

Gauges combine data from `self.pool_state` (role counts) and `self` (unmanaged
connections, lifecycle flags):

```python
pool_state = self.pool_state
gauges = HasqlGauges(
    master_count=pool_state.master_pool_count,
    replica_count=pool_state.replica_pool_count,
    available_count=pool_state.available_pool_count,
    active_connections=len(self._unmanaged_connections),
    closing=self._closing,
    closed=self._closed,
)
return Metrics(
    pools=pool_metrics,
    hasql=self._metrics.metrics(),
    gauges=gauges,
)
```

---

## Phase 5 — Update Tests ✅ DONE

### 5.1 Fix existing broken assertions

| Test file | Change |
|---|---|
| `tests/test_psycopg3.py::test_metrics` | `used=11` → `used=2` (Phase 1 bugfix) |

### 5.2 Update driver metric tests to use `PoolMetrics`

All tests that compare `metrics().drivers == [DriverMetrics(...)]` need updating to
use `metrics().pools == [PoolMetrics(...)]`. The new assertions can also verify
`role`, `healthy`, `response_time`, and `in_flight` fields.

Tests already access pool state via `.pool_state` (done in the PoolState refactor),
so no additional migration is needed for pool-state access patterns.

| Test file | Tests to update |
|---|---|
| `tests/test_aiopg.py` | `test_driver_context_metrics`, `test_driver_metrics` |
| `tests/test_aiopg_sa.py` | `test_metrics` |
| `tests/test_asyncpg.py` | `test_metrics` |
| `tests/test_asyncsqlalchemy.py` | `test_metrics` |
| `tests/test_psycopg3.py` | `test_metrics` |
| `tests/test_base_pool_manager.py` | `test_metrics_after_acquire` |

### 5.3 Add new tests

| Test | What it covers |
|---|---|
| `test_pool_metrics_role` | Verify `role="master"` for master pool, `role="replica"` for replica |
| `test_pool_metrics_unhealthy` | Verify `role=None, healthy=False` when pool is evicted from both sets |
| `test_pool_metrics_response_time` | Verify `response_time` is populated after health check runs |
| `test_pool_metrics_in_flight` | Verify `in_flight` increments on acquire, decrements on release |
| `test_pool_metrics_extra_psycopg3` | Verify `extra` contains psycopg3-specific keys (`requests_waiting`, `connections_errors`, etc.) |
| `test_pool_metrics_extra_sqlalchemy` | Verify `extra` contains `overflow` key |
| `test_gauges` | Verify `gauges.master_count`, `gauges.replica_count`, `gauges.active_connections`, etc. |
| `test_gauges_after_close` | Verify `gauges.closing` / `gauges.closed` reflect lifecycle |
| `test_deprecated_drivers_accessor` | Verify `Metrics.drivers` property still works and emits `DeprecationWarning` |
| `test_pool_stats_per_driver` | Unit-test each driver's `pool_stats()` independently with mock pools |

### 5.4 Update mock driver ✅ (done in Phase 2)

**File:** `tests/mocks/pool_manager.py`

`TestDriver` now implements `pool_stats()` returning real pool state:

```python
def pool_stats(self, pool):
    return PoolStats(
        min=0,
        max=len(pool.connections),
        idle=pool.freesize,
        used=len(pool.used),
    )
```

---

## Phase 6 — Update Example and Docs ✅ DONE

### 6.1 Fix example web server

**File:** `example/simple_web_server.py`

The `MetricsHandler` currently does `[asdict(m) for m in metrics]` — this iterates
over `Metrics` as if it's a sequence, which is wrong for the current API and will
remain wrong. Fix to:

```python
class MetricsHandler(BaseView):
    async def get(self):
        metrics = self.pool.metrics()
        return aiohttp.web.json_response(asdict(metrics))
```

### 6.2 Update CLAUDE.md architecture docs

Update the "Metrics and Monitoring" section to reflect:
- The new `PoolMetrics` model with role/health/response_time/in_flight/extra
- The `pool_stats()` driver method replacing `driver_metrics()`
- The `HasqlGauges` snapshot on `Metrics`
- That `PoolState` owns the pool state used for enrichment, while
  `BasePoolManager` owns `_unmanaged_connections` and `_metrics`

### 6.3 Add migration note to migration guide

**File:** `docs/migration-0.9.0-to-0.10.0.md` (merged from 0.9→0.10 + 0.10→0.11)

Document:
- `Metrics.drivers` → deprecated, use `Metrics.pools` (returns `Sequence[PoolMetrics]`)
- `PoolDriver.driver_metrics()` → deprecated, implement `pool_stats()` instead
- New fields available on `PoolMetrics`: `role`, `healthy`, `response_time`,
  `in_flight`, `extra`
- New `Metrics.gauges` field (`HasqlGauges`)
- `PoolState` / `PoolStateProvider` extraction (already released, document for
  completeness)

---

## Phase 7 — OTLP Metrics Examples ✅ DONE

> Add per-driver example showing how to scrape `Metrics` and export to an OTLP
> collector using the OpenTelemetry SDK. Each example is a standalone script that
> users can copy-paste into their project.

### 7.1 Shared OTLP helper module

**File:** `example/otlp/common.py`

A small helper that sets up an OTel `MeterProvider` with an
`OTLPMetricExporter` (gRPC, configurable via `OTEL_EXPORTER_OTLP_ENDPOINT`)
and provides a `record_hasql_metrics(meter, metrics)` function that maps `Metrics`
fields to OTel instruments:

| hasql field | OTel instrument | Attributes |
|---|---|---|
| `pool.min` | `UpDownCounter` `db.pool.connections.min` | `host`, `role` |
| `pool.max` | `UpDownCounter` `db.pool.connections.max` | `host`, `role` |
| `pool.idle` | `UpDownCounter` `db.pool.connections.idle` | `host`, `role` |
| `pool.used` | `UpDownCounter` `db.pool.connections.used` | `host`, `role` |
| `pool.in_flight` | `UpDownCounter` `db.pool.connections.in_flight` | `host`, `role` |
| `pool.response_time` | `Histogram` `db.pool.health_check.duration` | `host`, `role` |
| `pool.healthy` | `UpDownCounter` `db.pool.healthy` | `host`, `role` |
| `pool.extra.*` | driver-specific gauges (optional) | `host`, `role` |
| `gauges.master_count` | `UpDownCounter` `db.pool.masters` | — |
| `gauges.replica_count` | `UpDownCounter` `db.pool.replicas` | — |
| `gauges.active_connections` | `UpDownCounter` `db.pool.active_connections` | — |
| `hasql.acquire[host]` | `Counter` `db.pool.acquire.count` | `host` |
| `hasql.acquire_time[host]` | `Counter` `db.pool.acquire.duration` | `host` |

The function uses **observable gauges with callbacks** so that OTel's periodic
reader calls `pool_manager.metrics()` at the configured scrape interval instead of
requiring a manual push loop.

### 7.2 asyncpg + OTLP example

**File:** `example/otlp/asyncpg.py`

Standalone script: creates an `asyncpg.PoolManager`, registers an OTel observable
callback that calls `pool_manager.metrics()`, then runs a simple workload loop so
the user can see metrics flowing into their collector.

```
Dependencies: hasql, asyncpg, opentelemetry-sdk, opentelemetry-exporter-otlp-proto-grpc
Usage:        OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
              python example/otlp/asyncpg.py --dsn postgresql://u:p@db1,db2/mydb
```

### 7.3 psycopg3 + OTLP example

**File:** `example/otlp/psycopg3.py`

Same pattern as 7.2 but uses `psycopg3.PoolManager`. Additionally demonstrates
exporting psycopg3-specific `extra` keys (`requests_waiting`, `connections_errors`,
etc.) as dedicated OTel gauges.

### 7.4 aiopg + OTLP example

**File:** `example/otlp/aiopg.py`

Same pattern as 7.2 but uses `aiopg.PoolManager`.

### 7.5 asyncsqlalchemy + OTLP example

**File:** `example/otlp/asyncsqlalchemy.py`

Same pattern as 7.2 but uses `asyncsqlalchemy.PoolManager`. Shows the SQLAlchemy
`extra.overflow` gauge.

### 7.6 aiopg_sa + OTLP example

**File:** `example/otlp/aiopg_sa.py`

Same pattern as 7.2 but uses `aiopg_sa.PoolManager`.

---

## File Change Index

Summary of every file touched, grouped by phase.

| Phase | File | Action |
|-------|------|--------|
| 1.1 ✅ | `hasql/driver/psycopg3.py` | Fix `used=` computation |
| 1.1 ✅ | `tests/test_psycopg3.py` | Fix expected `used` value |
| 1.2 ✅ | `hasql/driver/asyncpg.py` | Fix `used=` computation |
| 2.1 ✅ | `hasql/metrics.py` | Add `PoolStats` dataclass |
| 2.2 ✅ | `hasql/abc.py` | Add `pool_stats()` abstract method |
| 2.3 ✅ | `hasql/driver/aiopg.py` | Implement `pool_stats()`, remove `driver_metrics()` override |
| 2.3 ✅ | `hasql/driver/aiopg_sa.py` | Implement `pool_stats()`, remove `driver_metrics()` override |
| 2.3 ✅ | `hasql/driver/asyncpg.py` | Implement `pool_stats()`, remove `driver_metrics()` override |
| 2.3 ✅ | `hasql/driver/psycopg3.py` | Implement `pool_stats()`, remove `driver_metrics()` override |
| 2.3 ✅ | `hasql/driver/asyncsqlalchemy.py` | Implement `pool_stats()`, remove `driver_metrics()` override |
| 2.4 ✅ | `hasql/abc.py` | Deprecate `driver_metrics()`, add default impl delegating to `pool_stats()` |
| 3.1 | `hasql/metrics.py` | Add `PoolMetrics` dataclass |
| 3.2 | `hasql/pool_manager.py` | Rewrite `metrics()` — iterate `pool_state._pools`, call `pool_stats()`, enrich via `pool_state` |
| 3.3 | `hasql/metrics.py` | Update `Metrics` — rename `drivers` → `pools`, add backward-compat `drivers` property |
| 3.4 | `hasql/pool_manager.py` | Remove `_driver_metrics()` proxy method |
| 4.1 | `hasql/metrics.py` | Add `HasqlGauges` dataclass |
| 4.2 | `hasql/metrics.py` | Add `gauges` field to `Metrics` |
| 4.3 | `hasql/pool_manager.py` | Build `HasqlGauges` in `metrics()` using `pool_state` properties + `self` state |
| 5.1 | `tests/test_psycopg3.py` | Already done in Phase 1 |
| 5.2 | `tests/test_aiopg.py` | Update to `PoolMetrics` assertions |
| 5.2 | `tests/test_aiopg_sa.py` | Update to `PoolMetrics` assertions |
| 5.2 | `tests/test_asyncpg.py` | Update to `PoolMetrics` assertions |
| 5.2 | `tests/test_asyncsqlalchemy.py` | Update to `PoolMetrics` assertions |
| 5.2 | `tests/test_base_pool_manager.py` | Update to `PoolMetrics` assertions |
| 5.3 | `tests/test_pool_metrics.py` | New file — role, health, response_time, in_flight, extra, gauges tests |
| 5.4 ✅ | `tests/mocks/pool_manager.py` | Add `pool_stats()` to `TestDriver` |
| 6.1 | `example/simple_web_server.py` | Fix `MetricsHandler` serialization |
| 6.2 | `CLAUDE.md` | Update architecture docs |
| 6.3 | `docs/migration-0.9.0-to-0.10.0.md` | Merged migration guide (0.9→0.11) |
| 7.1 ✅ | `example/otlp/common.py` | Shared OTLP helper — OTel setup + `register_hasql_metrics()` |
| 7.2 ✅ | `example/otlp/asyncpg.py` | asyncpg OTLP example |
| 7.3 ✅ | `example/otlp/psycopg3.py` | psycopg3 OTLP example |
| 7.4 ✅ | `example/otlp/aiopg.py` | aiopg OTLP example |
| 7.5 ✅ | `example/otlp/asyncsqlalchemy.py` | asyncsqlalchemy OTLP example |
| 7.6 ✅ | `example/otlp/aiopg_sa.py` | aiopg_sa OTLP example |

**Files NOT touched** (no changes needed thanks to the PoolState refactor):
- `hasql/pool_state.py` — already exposes `pool_is_master()`, `pool_is_replica()`,
  `get_last_response_time()`, `master_pool_count`, `replica_pool_count`,
  `available_pool_count` as public API. No modifications required.
- `hasql/balancer_policy/` — depends on `PoolStateProvider`, unaffected by metrics
  changes.
- `hasql/health.py` — accesses pool state via `manager.pool_state`, unaffected.

---

## Execution Order

Phases 1 and 2 have no dependencies on each other and can be developed in parallel
branches. Phases 3 and 4 depend on Phase 2. Phase 5 spans all phases (each phase
should include its own test updates). Phase 6 should be done last. Phase 7 depends
on Phases 3+4 (needs `PoolMetrics` and `HasqlGauges` to exist).

```
Phase 1 (bugfixes) ✅ ──────────────────────────────────────┐
                                                            ├─► Phase 5 + 6 ──► Phase 7 (OTLP examples)
Phase 2 (pool_stats ABC) ✅ ──► Phase 3 (PoolMetrics) ──► Phase 4 (gauges) ───┘
```

### Recommended release strategy

- **Patch release (0.10.1):** Phase 1 only — pure bugfixes, no API changes.
- **Minor release (0.10.0):** Phases 2–7 — new metrics API with deprecation path,
  OTLP examples. This release also includes the `PoolState` / `PoolStateProvider`
  extraction that has already been implemented.
