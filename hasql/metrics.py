import time
import warnings
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence


@dataclass(frozen=True)
class PoolStats:
    """Raw pool statistics returned by a driver for a single pool."""
    min: int
    max: int
    idle: int
    used: int
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DriverMetrics:
    max: int
    min: int
    idle: int
    used: int
    host: str


@dataclass(frozen=True)
class HasqlMetrics:
    pool: int
    pool_time: float
    acquire: Dict[str, int]
    acquire_time: Dict[str, float]
    add_connections: Dict[str, int]
    remove_connections: Dict[str, int]


@dataclass
class CalculateMetrics:
    _pool: int = 0
    _pool_time: float = 0.
    _acquire: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _acquire_time: Dict[str, float] = field(
        default_factory=lambda: defaultdict(int)
    )
    _add_connections: Dict[str, int] = field(default_factory=dict)
    _remove_connections: Dict[str, int] = field(default_factory=dict)

    def metrics(self) -> HasqlMetrics:
        return HasqlMetrics(
            pool=self._pool,
            pool_time=self._pool_time,
            acquire=self._acquire,
            acquire_time=self._acquire_time,
            add_connections=self._add_connections,
            remove_connections=self._remove_connections,
        )

    @contextmanager
    def with_get_pool(self):
        self._pool += 1
        tt = time.monotonic()
        yield
        self._pool_time += time.monotonic() - tt

    @contextmanager
    def with_acquire(self, pool: str):
        self._acquire[pool] += 1
        tt = time.monotonic()
        yield
        self._acquire_time[pool] += time.monotonic() - tt

    def add_connection(self, dsn: str):
        self._add_connections[dsn] = (
            self._add_connections.get(dsn, 0) + 1
        )

    def remove_connection(self, dsn: str):
        self._remove_connections[dsn] = (
            self._remove_connections.get(dsn, 0) + 1
        )


@dataclass(frozen=True)
class PoolMetrics:
    """Per-pool metrics, enriched by the pool manager."""
    host: str
    role: Optional[str]
    healthy: bool
    min: int
    max: int
    idle: int
    used: int
    response_time: Optional[float]
    in_flight: int
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HasqlGauges:
    """Point-in-time snapshot of pool manager state."""
    master_count: int
    replica_count: int
    available_count: int
    active_connections: int
    closing: bool
    closed: bool


@dataclass(frozen=True)
class Metrics:
    pools: Sequence[PoolMetrics]
    hasql: HasqlMetrics
    gauges: HasqlGauges

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
