"""Shared OTLP helper for hasql metrics.

Sets up an OpenTelemetry MeterProvider with an OTLP gRPC exporter and
registers observable gauges that scrape pool_manager.metrics() on the
configured collection interval.

Dependencies:
    opentelemetry-sdk
    opentelemetry-exporter-otlp-proto-grpc
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Sequence

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


def setup_meter_provider(
    export_interval_ms: int = 10_000,
) -> MeterProvider:
    """Create and install a MeterProvider with an OTLP gRPC exporter.

    The exporter reads OTEL_EXPORTER_OTLP_ENDPOINT from the environment
    (default: http://localhost:4317).
    """
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(),
        export_interval_millis=export_interval_ms,
    )
    provider = MeterProvider(metric_readers=[reader])
    set_meter_provider(provider)
    return provider


def _make_pool_gauge_cb(
    pool_manager: BasePoolManager,
    attr: str,
):
    def _cb(options: CallbackOptions) -> Iterable[Observation]:
        m = pool_manager.metrics()
        for p in m.pools:
            labels = {
                "host": p.host,
                "role": p.role or "unknown",
            }
            yield Observation(getattr(p, attr), labels)

    return _cb


def register_hasql_metrics(
    pool_manager: BasePoolManager,
    *,
    meter_name: str = "hasql",
) -> None:
    """Register observable gauges for pool_manager.metrics().

    Called once at startup. The periodic reader invokes the
    callbacks at the configured export interval.
    """
    meter = get_meter_provider().get_meter(meter_name)

    for name, attr, unit in [
        ("db.pool.connections.min", "min", "{connections}"),
        ("db.pool.connections.max", "max", "{connections}"),
        ("db.pool.connections.idle", "idle", "{connections}"),
        ("db.pool.connections.used", "used", "{connections}"),
        ("db.pool.connections.in_flight", "in_flight",
         "{connections}"),
    ]:
        meter.create_observable_gauge(
            name=name,
            callbacks=[_make_pool_gauge_cb(pool_manager, attr)],
            unit=unit,
        )

    def _healthy_cb(
        options: CallbackOptions,
    ) -> Iterable[Observation]:
        m = pool_manager.metrics()
        for p in m.pools:
            yield Observation(
                int(p.healthy),
                {"host": p.host, "role": p.role or "unknown"},
            )

    meter.create_observable_gauge(
        name="db.pool.healthy",
        callbacks=[_healthy_cb],
    )

    def _response_time_cb(
        options: CallbackOptions,
    ) -> Iterable[Observation]:
        m = pool_manager.metrics()
        for p in m.pools:
            if p.response_time is not None:
                yield Observation(
                    p.response_time,
                    {"host": p.host,
                     "role": p.role or "unknown"},
                )

    meter.create_observable_gauge(
        name="db.pool.health_check.duration",
        callbacks=[_response_time_cb],
        unit="s",
    )

    def _masters_cb(
        options: CallbackOptions,
    ) -> Iterable[Observation]:
        g = pool_manager.metrics().gauges
        yield Observation(g.master_count)

    def _replicas_cb(
        options: CallbackOptions,
    ) -> Iterable[Observation]:
        g = pool_manager.metrics().gauges
        yield Observation(g.replica_count)

    def _active_cb(
        options: CallbackOptions,
    ) -> Iterable[Observation]:
        g = pool_manager.metrics().gauges
        yield Observation(g.active_connections)

    meter.create_observable_gauge(
        name="db.pool.masters",
        callbacks=[_masters_cb],
    )
    meter.create_observable_gauge(
        name="db.pool.replicas",
        callbacks=[_replicas_cb],
    )
    meter.create_observable_gauge(
        name="db.pool.active_connections",
        callbacks=[_active_cb],
        unit="{connections}",
    )


def register_extra_gauges(
    pool_manager: BasePoolManager,
    extra_keys: Sequence[str],
    *,
    meter_name: str = "hasql",
) -> None:
    """Register observable gauges for driver-specific extra keys.

    Call this in addition to register_hasql_metrics() when you want to
    export driver-specific data (e.g. psycopg3's requests_waiting or
    SQLAlchemy's overflow).
    """
    meter = get_meter_provider().get_meter(meter_name)

    for key in extra_keys:

        def _make_cb(k: str):
            def _cb(options: CallbackOptions) -> Iterable[Observation]:
                m = pool_manager.metrics()
                for p in m.pools:
                    if k in p.extra:
                        yield Observation(
                            p.extra[k],
                            {"host": p.host, "role": p.role or "unknown"},
                        )

            return _cb

        meter.create_observable_gauge(
            name=f"db.pool.extra.{key}",
            callbacks=[_make_cb(key)],
        )
