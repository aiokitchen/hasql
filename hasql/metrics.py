import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Sequence


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
    acquire: int
    acquire_time: float
    add_connections: dict[str, int]
    remove_connections: dict[str, int]


@dataclass
class CalculateMetrics:
    _pool: int = 0
    _pool_time: int = 0
    _acquire: int = 0
    _acquire_time: int = 0
    _add_connections: dict[str, int] = field(default_factory=dict)
    _remove_connections: dict[str, int] = field(default_factory=dict)

    def metrics(self) -> HasqlMetrics:
        return HasqlMetrics(
            pool=self._pool,
            pool_time=self._pool_time,
            acquire=self._acquire,
            acquire_time=self._acquire,
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
    def with_acquire(self):
        self._acquire += 1
        tt = time.monotonic()
        yield
        self._acquire_time += time.monotonic() - tt

    def add_connection(self, dsn: str):
        self._add_connections[dsn] = (
            self._add_connections.get(dsn, 0) + 1
        )

    def remove_connection(self, dsn: str):
        self._remove_connections[dsn] = (
            self._remove_connections.get(dsn, 0) + 1
        )


@dataclass(frozen=True)
class Metrics:
    drivers: Sequence[DriverMetrics]
    hasql: HasqlMetrics
