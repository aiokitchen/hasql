"""Shared OTLP helper for hasql metrics.

Dependencies (examples only):
    opentelemetry-sdk
    opentelemetry-exporter-otlp-proto-grpc
"""

from __future__ import annotations

from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable, Sequence

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.metrics import (
    CallbackOptions,
    Observation,
    get_meter_provider,
    set_meter_provider,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

if TYPE_CHECKING:
    from hasql.pool_manager import BasePoolManager


def setup_meter_provider(export_interval_ms: int = 10_000) -> MeterProvider:
    """Create and install an OTLP gRPC MeterProvider."""
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(),
        export_interval_millis=export_interval_ms,
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


def _pool_attributes(pool: Any) -> dict[str, str]:
    attributes = {
        "host": str(pool.host),
        "role": _string_value(pool.role),
    }
    if pool.staleness is not None:
        attributes["staleness"] = _string_value(pool.staleness)
    return attributes


def _make_pool_gauge_cb(pool_manager: BasePoolManager, attr: str):
    def _cb(options: CallbackOptions) -> Iterable[Observation]:
        for pool in pool_manager.metrics().pools:
            yield Observation(getattr(pool, attr), _pool_attributes(pool))

    return _cb


def register_hasql_metrics(  # noqa: C901
    pool_manager: BasePoolManager,
    *,
    meter_name: str = "hasql",
) -> None:
    """Register point-in-time gauges and cumulative acquire counters."""
    meter = get_meter_provider().get_meter(meter_name)

    for name, attr, unit in [
        ("db.pool.connections.min", "min", "{connections}"),
        ("db.pool.connections.max", "max", "{connections}"),
        ("db.pool.connections.idle", "idle", "{connections}"),
        ("db.pool.connections.used", "used", "{connections}"),
        ("db.pool.connections.in_flight", "in_flight", "{connections}"),
    ]:
        meter.create_observable_gauge(
            name=name,
            callbacks=[_make_pool_gauge_cb(pool_manager, attr)],
            unit=unit,
        )

    def _healthy_cb(options: CallbackOptions) -> Iterable[Observation]:
        for pool in pool_manager.metrics().pools:
            yield Observation(int(pool.healthy), _pool_attributes(pool))

    def _response_time_cb(
        options: CallbackOptions,
    ) -> Iterable[Observation]:
        for pool in pool_manager.metrics().pools:
            if pool.response_time is not None:
                yield Observation(
                    pool.response_time,
                    _pool_attributes(pool),
                )

    meter.create_observable_gauge(
        name="db.pool.healthy",
        callbacks=[_healthy_cb],
    )
    meter.create_observable_gauge(
        name="db.pool.health_check.duration",
        callbacks=[_response_time_cb],
        unit="s",
    )

    gauge_callbacks = {
        "db.pool.masters": lambda metrics: metrics.gauges.master_count,
        "db.pool.replicas": lambda metrics: metrics.gauges.replica_count,
        "db.pool.active_connections": (
            lambda metrics: metrics.gauges.active_connections
        ),
        "db.pool.stale.count": lambda metrics: metrics.gauges.stale_count,
    }
    for name, getter in gauge_callbacks.items():
        def _make_cb(value_getter):
            def _cb(options: CallbackOptions) -> Iterable[Observation]:
                yield Observation(value_getter(pool_manager.metrics()))

            return _cb

        meter.create_observable_gauge(
            name=name,
            callbacks=[_make_cb(getter)],
        )

    def _acquire_count_cb(
        options: CallbackOptions,
    ) -> Iterable[Observation]:
        for host, count in pool_manager.metrics().hasql.acquire.items():
            yield Observation(count, {"host": str(host)})

    def _acquire_duration_cb(
        options: CallbackOptions,
    ) -> Iterable[Observation]:
        for host, duration in (
            pool_manager.metrics().hasql.acquire_time.items()
        ):
            yield Observation(duration, {"host": str(host)})

    meter.create_observable_counter(
        name="db.pool.acquire.count",
        callbacks=[_acquire_count_cb],
        unit="{acquisitions}",
    )
    meter.create_observable_counter(
        name="db.pool.acquire.duration",
        callbacks=[_acquire_duration_cb],
        unit="s",
    )

    def _stale_status_cb(
        options: CallbackOptions,
    ) -> Iterable[Observation]:
        for pool in pool_manager.metrics().pools:
            if pool.staleness is not None:
                yield Observation(
                    int(_string_value(pool.staleness) == "stale"),
                    _pool_attributes(pool),
                )

    def _lag_cb(key: str):
        def _cb(options: CallbackOptions) -> Iterable[Observation]:
            for pool in pool_manager.metrics().pools:
                if key not in pool.lag:
                    continue
                lag = pool.lag[key]
                if isinstance(lag, timedelta):
                    lag = lag.total_seconds()
                yield Observation(lag, _pool_attributes(pool))

        return _cb

    meter.create_observable_gauge(
        name="db.pool.stale.status",
        callbacks=[_stale_status_cb],
    )
    meter.create_observable_gauge(
        name="db.pool.stale.lag.bytes",
        callbacks=[_lag_cb("bytes")],
        unit="By",
    )
    meter.create_observable_gauge(
        name="db.pool.stale.lag.time",
        callbacks=[_lag_cb("time")],
        unit="s",
    )


def register_extra_gauges(
    pool_manager: BasePoolManager,
    extra_keys: Sequence[str],
    *,
    meter_name: str = "hasql",
) -> None:
    """Register driver-specific ``db.pool.extra.<key>`` gauges."""
    meter = get_meter_provider().get_meter(meter_name)
    for key in extra_keys:
        def _make_cb(extra_key: str):
            def _cb(options: CallbackOptions) -> Iterable[Observation]:
                for pool in pool_manager.metrics().pools:
                    if extra_key in pool.extra:
                        yield Observation(
                            pool.extra[extra_key],
                            _pool_attributes(pool),
                        )

            return _cb

        meter.create_observable_gauge(
            name=f"db.pool.extra.{key}",
            callbacks=[_make_cb(key)],
        )
