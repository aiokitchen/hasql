"""Owner-loop sampling and detached OTLP observations (examples only).

Requires opentelemetry-sdk and opentelemetry-exporter-otlp-proto-grpc.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from enum import Enum
from math import isfinite
from threading import Lock
from types import MappingProxyType
from typing import (
    TYPE_CHECKING, Any, AsyncIterator, Callable, Iterable, Mapping, Sequence,
)

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.metrics import (
    CallbackOptions, Meter, Observation, get_meter_provider, set_meter_provider,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

if TYPE_CHECKING:
    from hasql.pool_manager import BasePoolManager


log = logging.getLogger(__name__)
Attributes = tuple[tuple[str, str], ...]
Point = tuple[int | float, Attributes]
Snapshot = Mapping[str, tuple[Point, ...]]
Callback = Callable[[CallbackOptions], Iterable[Observation]]

_POOL_GAUGES = (
    ("db.pool.connections.min", "min", "{connections}"),
    ("db.pool.connections.max", "max", "{connections}"),
    ("db.pool.connections.idle", "idle", "{connections}"),
    ("db.pool.connections.used", "used", "{connections}"),
    ("db.pool.connections.in_flight", "in_flight", "{connections}"),
    ("db.pool.healthy", "healthy", ""),
    ("db.pool.health_check.duration", "response_time", "s"),
)
_MANAGER_GAUGES = (
    ("db.pool.masters", "master_count"),
    ("db.pool.replicas", "replica_count"),
    ("db.pool.active_connections", "active_connections"),
    ("db.pool.stale.count", "stale_count"),
)
_COUNTERS = (
    ("db.pool.acquire.count", "acquire", "{acquisitions}"),
    ("db.pool.acquire.duration", "acquire_time", "s"),
)
_LAG_GAUGES = (
    ("db.pool.stale.lag.bytes", "bytes", "By"),
    ("db.pool.stale.lag.time", "time", "s"),
)


def setup_meter_provider(export_interval_ms: int = 10_000) -> MeterProvider:
    """Install a provider with bounded export and SDK shutdown timeouts."""
    if not isfinite(export_interval_ms) or export_interval_ms <= 0:
        raise ValueError("export_interval_ms must be positive and finite")
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(timeout=10),
        export_interval_millis=export_interval_ms,
        export_timeout_millis=10_000,
    )
    provider = MeterProvider(metric_readers=[reader])
    set_meter_provider(provider)
    return provider


def _string_value(value: Any, default: str = "unknown") -> str:
    if value is None:
        return default
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def _sample(  # noqa: C901 -- project the fixed metric catalog in one pass
    pool_manager: BasePoolManager, extra_keys: Sequence[str],
) -> Snapshot:
    """Read once, projecting all mutable driver data before leaving the loop."""
    metrics = pool_manager.metrics()
    points: dict[str, list[Point]] = {}

    def add(name: str, value: int | float, attrs: Attributes = ()) -> None:
        if not isinstance(value, (int, float)):
            raise TypeError(
                f"Non-numeric metric {name}: {type(value).__name__}",
            )
        number = int(value) if isinstance(value, int) else float(value)
        points.setdefault(name, []).append((number, attrs))

    for pool in metrics.pools:
        attrs = (("host", str(pool.host)), ("role", _string_value(pool.role)))
        if pool.staleness is not None:
            attrs += (("staleness", _string_value(pool.staleness)),)
            add("db.pool.stale.status", int(
                _string_value(pool.staleness) == "stale",
            ), attrs)
        for name, attr, _unit in _POOL_GAUGES:
            value = getattr(pool, attr)
            if value is not None:
                add(name, int(value) if attr == "healthy" else value, attrs)
        for name, key, _unit in _LAG_GAUGES:
            if key in pool.lag:
                lag = pool.lag[key]
                add(name, lag.total_seconds() if isinstance(
                    lag, timedelta,
                ) else lag, attrs)
        for key in extra_keys:
            if key in pool.extra:
                add(f"db.pool.extra.{key}", pool.extra[key], attrs)
    for name, attr in _MANAGER_GAUGES:
        add(name, getattr(metrics.gauges, attr))
    for name, attr, _unit in _COUNTERS:
        for host, value in getattr(metrics.hasql, attr).items():
            add(name, value, (("host", str(host)),))
    return MappingProxyType({
        name: tuple(rows) for name, rows in points.items()
    })


class _SnapshotStore:
    _snapshot: Snapshot
    _lock: Lock

    def __init__(self, snapshot: Snapshot) -> None:
        self._snapshot = snapshot
        self._lock = Lock()

    def publish(self, snapshot: Snapshot) -> None:
        with self._lock:
            self._snapshot = snapshot

    def callback(self, name: str) -> Callback:
        def observe(options: CallbackOptions) -> Iterable[Observation]:
            with self._lock:
                snapshot = self._snapshot
            for value, attrs in snapshot.get(name, ()):
                yield Observation(value, dict(attrs))

        return observe


@asynccontextmanager
async def observe_hasql_metrics(
    pool_manager: BasePoolManager,
    *,
    sample_interval: float = 1.0,
    extra_keys: Sequence[str] = (),
    meter_name: str = "hasql",
    meter: Meter | None = None,
) -> AsyncIterator[None]:
    """Sample on the owning loop; callbacks only read the latest snapshot.

    Sampling and export intervals are independent. Each callback retains one
    snapshot, but a collection across instruments is not an atomic batch.
    The caller owns the provider and must shut it down separately.
    """
    if not isfinite(sample_interval) or sample_interval <= 0:
        raise ValueError("sample_interval must be positive and finite")
    extra_keys = tuple(extra_keys)
    store = _SnapshotStore(_sample(pool_manager, extra_keys))
    if meter is None:
        meter = get_meter_provider().get_meter(meter_name)
    gauges = (
        tuple((name, unit) for name, _attr, unit in _POOL_GAUGES)
        + tuple((name, "") for name, _attr in _MANAGER_GAUGES)
        + (("db.pool.stale.status", ""),)
        + tuple((name, unit) for name, _key, unit in _LAG_GAUGES)
        + tuple((f"db.pool.extra.{key}", "") for key in extra_keys)
    )
    for name, unit in gauges:
        meter.create_observable_gauge(
            name=name, callbacks=[store.callback(name)], unit=unit,
        )
    for name, _attr, unit in _COUNTERS:
        meter.create_observable_counter(
            name=name, callbacks=[store.callback(name)], unit=unit,
        )

    async def sample_periodically() -> None:
        while True:
            await asyncio.sleep(sample_interval)
            try:
                snapshot = _sample(pool_manager, extra_keys)
            except Exception:
                log.exception("Failed to sample hasql metrics")
                snapshot = MappingProxyType({})
            store.publish(snapshot)

    sampler = asyncio.create_task(sample_periodically())
    try:
        yield
    finally:
        sampler.cancel()
        with suppress(asyncio.CancelledError):
            await sampler
