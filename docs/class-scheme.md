# hasql Class Scheme & Areas of Responsibility

## Class Dependency Graph

```
                         ┌──────────────────────┐
                         │   User / Application │
                         └──────────┬───────────┘
                                    │ uses
                         ┌──────────▼───────────┐
                         │  Driver PoolManager  │  (hasql.driver.asyncpg.PoolManager, etc.)
                         │  thin constructor    │
                         └──────────┬───────────┘
                                    │ extends
                         ┌──────────▼───────────┐
                         │  BasePoolManager     │
                         │  [PoolT, ConnT]      │
                         └──┬───┬───┬───┬───┬───┘
                composes    │   │   │   │   │
           ┌────────────────┘   │   │   │   └────────────────┐
           ▼                    ▼   │   ▼                    ▼
   ┌───────────────┐  ┌────────────┐│ ┌──────────────┐ ┌──────────────┐
   │  PoolState    │  │PoolHealth- ││ │AbstractBal-  │ │Calculate-    │
   │  [PoolT,ConnT]│  │Monitor     ││ │ancerPolicy   │ │Metrics       │
   └───┬───────────┘  │[PoolT,ConnT││ │[PoolT]       │ └──────────────┘
       │              └────────────┘│ └──────┬───────┘
       │ composes                   │        │ depends on
       ▼                            │        ▼
   ┌───────────────┐                │ ┌───────────────────┐
   │  Stopwatch    │                │ │PoolStateProvider  │ (Protocol)
   │  [KeyT]       │                │ │[PoolT]            │
   └───────────────┘                │ └───────────────────┘
                                    │        ▲
                                    │        │ 
                                    │  implements PoolState
                                    │
                             ┌──────▼───────┐
                             │ PoolDriver   │  (ABC)
                             │ [PoolT,ConnT]│
                             └──────┬───────┘
                                    │ implemented by
             ┌──────────┬───────────┼───────────┬──────────────┐
             ▼          ▼           ▼           ▼              ▼
        ┌──────────┐┌──────────┐┌──────────┐┌───────────┐┌──────────┐
        │Asyncpg-  ││Aiopg-    ││Psycopg3- ││AsyncSql-  ││AiopgSa-  │
        │Driver    ││Driver    ││Driver    ││AlchemyDr. ││Driver    │
        └──────────┘└──────────┘└──────────┘└───────────┘└────┬─────┘
                                                              │ extends
                                                         AiopgDriver
```

## Acquire Path

```
pool_manager.acquire(read_only=...)
    └─► PoolAcquireContext.__aenter__()
            ├─► balancer.get_pool(...)           # select pool
            │       └─► _get_candidates(...)     # from PoolStateProvider
            │       └─► _get_pool(...)           # policy-specific selection
            ├─► pool_state.acquire_from_pool(pool) # get connection
            │       └─► (may wrap in TimeoutAcquireContext)
            └─► pool_manager._register_connection(conn, pool)
```

## Areas of Responsibility

### 1. `PoolDriver[PoolT, ConnT]` — Driver Abstraction (`hasql/abc.py`)

Translate hasql operations into driver-specific API calls.

| Owns | Does NOT own |
|---|---|
| Pool creation (`pool_factory`) | Pool lifecycle orchestration |
| Connection acquire/release | Connection tracking (who holds what) |
| Master detection query (`is_master`) | Health monitoring schedule |
| Raw pool stats (`pool_stats`) | Metrics enrichment |
| Host extraction from pool | Role assignment (master/replica sets) |
| Pool close/terminate | Decision of when to close |

Concrete implementations: `AsyncpgDriver`, `AiopgDriver`, `Psycopg3Driver`, `AsyncSqlAlchemyDriver`, `AiopgSaDriver`.

Each driver adjusts pool sizes (+1 for system connection) via `prepare_pool_factory_kwargs`.

### 2. `PoolState[PoolT, ConnT]` — Pool State Management (`hasql/pool_state.py`)

Single point of contact with the driver. All driver operations go through PoolState; neither the manager nor the health monitor call the driver directly.

| Owns | Does NOT own |
|---|---|
| Driver gateway (all driver calls) | Deciding when to check roles |
| Master/replica pool sets | Background health tasks |
| Adding/removing pools from role sets | Connection acquisition (user-facing) |
| Waiting primitives (`asyncio.Condition`) | Load balancing decisions |
| Pool readiness events | Metrics collection |
| Response time tracking (via `Stopwatch`) | |
| `PoolStateProvider` protocol implementation | |

### 3. `BasePoolManager[PoolT, ConnT]` — Orchestrator (`hasql/pool_manager.py`)

Compose all subsystems and expose the user-facing API (acquire/release/metrics/lifecycle).

| Owns | Does NOT own |
|---|---|
| Composing PoolState + Health + Balancer + Metrics | Pool state mutation logic (delegates to PoolState) |
| Connection tracking (`_unmanaged_connections` dict) | Driver-specific behavior (delegates to PoolState → Driver) |
| `acquire()` public API | Balancer selection algorithm |
| `metrics()` — enriching raw stats into `PoolMetrics` | Health check scheduling (delegates to HealthMonitor) |
| Lifecycle: `close()` | Raw pool stats (delegates to PoolState) |
| Periodic pool check (`_periodic_pool_check`) | Background task management |
| Context manager (`__aenter__`/`__aexit__`) | |

### 4. `PoolHealthMonitor[PoolT, ConnT]` — Background Health Checking (`hasql/health.py`)

Run background `asyncio.Task` per DSN that periodically creates pools, checks roles, and updates state.

| Owns | Does NOT own |
|---|---|
| Background task lifecycle (start/stop) | Role detection logic (calls back to manager) |
| One task per DSN | Pool state mutation (calls PoolState) |
| Waiting for pool creation | Connection acquisition for user requests |
| Safe connection release on errors | |
| Notifying pool-check condition after each check | |

### 5. `AbstractBalancerPolicy[PoolT]` — Pool Selection Strategy (`hasql/balancer_policy/base.py`)

Given a set of candidate pools, select one according to a strategy.

| Owns | Does NOT own |
|---|---|
| `master_as_replica_weight` randomization logic | Pool state (reads via `PoolStateProvider`) |
| Building candidate list (`_get_candidates`) | Connection acquisition |
| Abstract selection (`_get_pool`) | Health monitoring |

Depends on `PoolStateProvider` protocol (no direct import of `BasePoolManager`).

#### Concrete Policies

| Policy | Strategy | Key data used |
|---|---|---|
| `GreedyBalancerPolicy` | Pick pool(s) with max free connections, random tie-break | `get_pool_freesize()` |
| `RandomWeightedBalancerPolicy` | Weighted random — faster = higher probability | `get_last_response_time()` |
| `RoundRobinBalancerPolicy` | Sequential cycling, index per `(read_only, choose_master_as_replica)` | candidate list order |

### 6. `PoolAcquireContext[PoolT, ConnT]` — Acquire Transaction (`hasql/acquire.py`)

Execute the full acquire flow (select pool → get connection → register → yield → release) as an async context manager.

| Owns | Does NOT own |
|---|---|
| Deadline/timeout tracking | Pool selection algorithm |
| Retrying pool selection within timeout budget | Driver-specific acquire behavior |
| Entering/exiting driver's acquire context | Connection tracking registry |
| Registering/unregistering connection via manager | |
| Supporting both `async with` and `await` patterns | |

Supporting classes:
- **`AcquireContext[ConnT_co]`** (Protocol) — Defines the async context manager + awaitable interface that drivers must return.
- **`TimeoutAcquireContext[ConnT]`** — Wraps any `AcquireContext` with `asyncio.wait_for` timeout. Used by aiopg and SQLAlchemy drivers that don't natively support timeouts.

### 7. Metrics Model (`hasql/metrics.py`)

Define the metrics data model and internal tracking.

```
Driver returns          Manager enriches         User receives
─────────────          ─────────────────         ─────────────
PoolStats ──────────►  PoolMetrics ──────────►   Metrics
(raw: min/max/         (+role, healthy,          (pools + hasql + gauges)
 idle/used/extra)       response_time,
                        in_flight)
```

| Class | Role |
|---|---|
| `PoolStats` | Raw per-pool stats from driver (frozen dataclass) |
| `PoolMetrics` | Enriched per-pool metrics with role, health, timing (frozen dataclass) |
| `HasqlMetrics` | Aggregated acquire/pool-selection timing and counts (frozen dataclass) |
| `HasqlGauges` | Point-in-time manager state snapshot (frozen dataclass) |
| `Metrics` | Top-level container: `pools` + `hasql` + `gauges` (frozen dataclass) |
| `CalculateMetrics` | Mutable internal tracker with context managers for timing (mutable dataclass) |
| `DriverMetrics` | Deprecated backward-compat wrapper |

### 8. `Dsn` — Connection String Parsing (`hasql/utils.py`)

Parse, manipulate, and compile PostgreSQL connection strings (both URL and libpq formats).

| Owns | Does NOT own |
|---|---|
| Parsing multi-host DSN strings | Splitting into individual DSNs (see `split_dsn`) |
| Building netloc from hosts/ports | Creating pools from DSNs |
| `with_()` — creating modified copies | |
| IPv6 host detection | |

`split_dsn()` (standalone function) splits a multi-host `Dsn` into `List[Dsn]`, one per host.

### 9. `Stopwatch[KeyT]` — Response Time Tracker (`hasql/utils.py`)

Track operation durations per key using a sliding window and provide cached median values.

Used by `PoolState` to track per-pool response times. Consumed by `RandomWeightedBalancerPolicy` for weighting.

### 10. Driver-Specific `PoolManager` Classes

Each lives in `hasql/driver/<name>.py`.

Thin constructor that creates the appropriate `PoolDriver` and passes it to `BasePoolManager.__init__`.

```python
# Example: hasql/driver/asyncpg.py
class PoolManager(BasePoolManager[asyncpg.Pool, asyncpg.Connection]):
    def __init__(self, dsn: str, **kwargs):
        super().__init__(dsn, driver=AsyncpgDriver(), **kwargs)
```

No additional methods or overrides — purely a convenience binding.

## Circular Import Resolution

```
pool_state.py ──imports──► utils.py, abc.py, acquire.py, metrics.py
balancer_policy/base.py ──imports──► pool_state.py (PoolStateProvider only)
pool_manager.py ──imports──► pool_state.py, balancer_policy/, health.py, metrics.py, abc.py, acquire.py
health.py ──TYPE_CHECKING import──► pool_manager.py (BasePoolManager)
acquire.py ──TYPE_CHECKING import──► balancer_policy/base.py, pool_state.py
```

The key insight: `AbstractBalancerPolicy` depends on `PoolStateProvider` (a Protocol in `pool_state.py`), not on `BasePoolManager`. This breaks what would otherwise be a circular import between `pool_manager.py` ↔ `balancer_policy/`.
