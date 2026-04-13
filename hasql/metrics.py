import time
import warnings
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from collections.abc import Sequence
from typing import Any


class PoolRole(str, Enum):
    MASTER = "master"
    REPLICA = "replica"


class PoolStaleness(str, Enum):
    FRESH = "fresh"
    STALE = "stale"


@dataclass(frozen=True)
class PoolStats:
    """Raw pool statistics returned by a driver for a single pool."""
    min: int
    max: int
    idle: int
    used: int
    extra: dict[str, Any] = field(default_factory=dict)


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
    acquire: dict[str, int]
    acquire_time: dict[str, float]
    add_connections: dict[str, int]
    remove_connections: dict[str, int]


@dataclass
class CalculateMetrics:
    _pool: int = 0
    _pool_time: float = 0.
    _acquire: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _acquire_time: dict[str, float] = field(
        default_factory=lambda: defaultdict(float)
    )
    _add_connections: dict[str, int] = field(default_factory=dict)
    _remove_connections: dict[str, int] = field(default_factory=dict)

    def metrics(self) -> HasqlMetrics:
        return HasqlMetrics(
            pool=self._pool,
            pool_time=self._pool_time,
            acquire=dict(self._acquire),
            acquire_time=dict(self._acquire_time),
            add_connections=dict(self._add_connections),
            remove_connections=dict(self._remove_connections),
        )

    @contextmanager
    def with_get_pool(self):
        self._pool += 1
        tt = time.monotonic()
        try:
            yield
        finally:
            self._pool_time += time.monotonic() - tt

    @contextmanager
    def with_acquire(self, host: str):
        self._acquire[host] += 1
        tt = time.monotonic()
        try:
            yield
        finally:
            self._acquire_time[host] += time.monotonic() - tt

    def add_connection(self, host: str):
        self._add_connections[host] = (
            self._add_connections.get(host, 0) + 1
        )

    def remove_connection(self, host: str):
        self._remove_connections[host] = (
            self._remove_connections.get(host, 0) + 1
        )


@dataclass(frozen=True)
class PoolMetrics:
    """Per-pool metrics, enriched by the pool manager."""
    host: str
    role: PoolRole | None
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


__all__ = (
    "PoolRole",
    "PoolStaleness",
    "PoolStats",
    "DriverMetrics",
    "HasqlMetrics",
    "CalculateMetrics",
    "PoolMetrics",
    "HasqlGauges",
    "Metrics",
)
