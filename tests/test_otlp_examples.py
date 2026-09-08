"""Example metrics ownership and lifecycle; real SDK, no remote collector."""

import asyncio
import importlib
import subprocess
import sys
import threading
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from opentelemetry.metrics import CallbackOptions
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    InMemoryMetricReader,
    MetricExporter,
    MetricExportResult,
    PeriodicExportingMetricReader,
    Sum,
)

from example.otlp import common
from hasql.metrics import (
    HasqlGauges,
    HasqlMetrics,
    Metrics,
    PoolMetrics,
    PoolRole,
    PoolStaleness,
)


def _snapshot():
    return Metrics(
        pools=[PoolMetrics(
            host="primary", role=PoolRole.REPLICA, healthy=True,
            min=1, max=5, idle=3, used=2, in_flight=1,
            response_time=0.25, staleness=PoolStaleness.STALE,
            lag={"bytes": 32, "time": timedelta(seconds=2)},
            extra={"overflow": 4},
        )],
        hasql=HasqlMetrics(
            pool=1, pool_time=0.5, acquire={"primary": 7},
            acquire_time={"primary": 1.5},
            add_connections={}, remove_connections={},
        ),
        gauges=HasqlGauges(
            master_count=1, replica_count=2, available_count=3,
            active_connections=1, closing=False, closed=False, stale_count=1,
        ),
    )


class _OwnerManager:
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.owner_thread = threading.get_ident()
        self.foreign_threads = []
        self.snapshot = _snapshot()
        self.calls = 0
        self.sampled = asyncio.Event()
        self.error = None

    def metrics(self):
        self.calls += 1
        if threading.get_ident() != self.owner_thread:
            self.foreign_threads.append(threading.get_ident())
            raise RuntimeError("metrics accessed outside owner thread")
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("metrics accessed outside owner loop")
        self.sampled.set()
        if self.error is not None:
            raise self.error
        return self.snapshot


@asynccontextmanager
async def _sdk_meter(reader=None):
    reader = reader or InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    try:
        yield reader, provider.get_meter("hasql-test"), provider
    finally:
        await asyncio.to_thread(provider.shutdown)


async def test_callbacks_never_read_manager_from_collection_thread():
    manager = _OwnerManager()
    async with _sdk_meter() as (reader, meter, _provider):
        async with common.observe_hasql_metrics(
            manager, sample_interval=60, extra_keys=("overflow",), meter=meter,
        ):
            await asyncio.to_thread(reader.get_metrics_data)
            await asyncio.to_thread(reader.get_metrics_data)

    assert (manager.foreign_threads, manager.calls) == ([], 1)


class _RecordingMeter:
    """Thin tap around a real SDK meter, not an SDK replacement."""

    def __init__(self, meter, manager):
        self.meter = meter
        self.manager = manager
        self.callbacks = {}
        self.calls_at_registration = []

    def _create(self, factory, name, callbacks, **kwargs):
        self.callbacks[name] = tuple(callbacks)
        self.calls_at_registration.append(self.manager.calls)
        return factory(name, callbacks=callbacks, **kwargs)

    def create_observable_gauge(self, name, callbacks, **kwargs):
        return self._create(
            self.meter.create_observable_gauge, name, callbacks, **kwargs,
        )

    def create_observable_counter(self, name, callbacks, **kwargs):
        return self._create(
            self.meter.create_observable_counter, name, callbacks, **kwargs,
        )


def _all_observations(meter):
    # Creation AND full generator iteration happen in the calling worker.
    return {
        name: tuple(
            observation
            for callback in callbacks
            for observation in callback(CallbackOptions())
        )
        for name, callbacks in meter.callbacks.items()
    }


def _observed_values(meter):
    return {
        name: tuple((item.value, dict(item.attributes or {})) for item in items)
        for name, items in _all_observations(meter).items()
    }


def _sdk_catalog(reader):
    data = reader.get_metrics_data()
    return {
        metric.name: (
            metric.unit,
            type(metric.data).__name__,
            tuple(
                (point.value, dict(point.attributes))
                for point in metric.data.data_points
            ),
        )
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }


async def _next_sample(manager):
    manager.sampled.clear()
    await asyncio.wait_for(manager.sampled.wait(), timeout=2)


async def test_initial_sample_precedes_all_instrument_registrations():
    manager = _OwnerManager()
    async with _sdk_meter() as (_reader, meter, _provider):
        recording = _RecordingMeter(meter, manager)
        async with common.observe_hasql_metrics(
            manager, sample_interval=60, meter=recording,
        ):
            calls = recording.calls_at_registration

    assert set(calls) == {1}


async def test_real_sdk_metric_names_types_units_and_values():
    manager = _OwnerManager()
    attributes = {
        "host": "primary", "role": "replica", "staleness": "stale",
    }
    async with _sdk_meter() as (reader, meter, _provider):
        async with common.observe_hasql_metrics(
            manager, sample_interval=60, extra_keys=("overflow",), meter=meter,
        ):
            catalog = await asyncio.to_thread(_sdk_catalog, reader)

    assert catalog == {
        "db.pool.connections.min": (
            "{connections}", "Gauge", ((1, attributes),),
        ),
        "db.pool.connections.max": (
            "{connections}", "Gauge", ((5, attributes),),
        ),
        "db.pool.connections.idle": (
            "{connections}", "Gauge", ((3, attributes),),
        ),
        "db.pool.connections.used": (
            "{connections}", "Gauge", ((2, attributes),),
        ),
        "db.pool.connections.in_flight": (
            "{connections}", "Gauge", ((1, attributes),),
        ),
        "db.pool.healthy": ("", "Gauge", ((1, attributes),)),
        "db.pool.health_check.duration": ("s", "Gauge", ((0.25, attributes),)),
        "db.pool.masters": ("", "Gauge", ((1, {}),)),
        "db.pool.replicas": ("", "Gauge", ((2, {}),)),
        "db.pool.active_connections": ("", "Gauge", ((1, {}),)),
        "db.pool.stale.count": ("", "Gauge", ((1, {}),)),
        "db.pool.acquire.count": (
            "{acquisitions}", "Sum", ((7, {"host": "primary"}),),
        ),
        "db.pool.acquire.duration": (
            "s", "Sum", ((1.5, {"host": "primary"}),),
        ),
        "db.pool.stale.status": ("", "Gauge", ((1, attributes),)),
        "db.pool.stale.lag.bytes": ("By", "Gauge", ((32, attributes),)),
        "db.pool.stale.lag.time": ("s", "Gauge", ((2.0, attributes),)),
        "db.pool.extra.overflow": ("", "Gauge", ((4, attributes),)),
    }


def _sdk_counters(reader):
    data = reader.get_metrics_data()
    return {
        metric.name: (
            metric.data.is_monotonic,
            metric.data.aggregation_temporality,
            tuple(point.value for point in metric.data.data_points),
        )
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if isinstance(metric.data, Sum)
    }


async def test_acquire_counters_remain_cumulative_across_collections():
    manager = _OwnerManager()
    async with _sdk_meter() as (reader, meter, _provider):
        async with common.observe_hasql_metrics(
            manager, sample_interval=0.01, meter=meter,
        ):
            first = await asyncio.to_thread(_sdk_counters, reader)
            manager.snapshot.hasql.acquire["primary"] = 9
            manager.snapshot.hasql.acquire_time["primary"] = 2.5
            await _next_sample(manager)
            second = await asyncio.to_thread(_sdk_counters, reader)
            repeated = await asyncio.to_thread(_sdk_counters, reader)

    assert (first, second, repeated) == (
        {
            "db.pool.acquire.count": (
                True, AggregationTemporality.CUMULATIVE, (7,),
            ),
            "db.pool.acquire.duration": (
                True, AggregationTemporality.CUMULATIVE, (1.5,),
            ),
        },
        {
            "db.pool.acquire.count": (
                True, AggregationTemporality.CUMULATIVE, (9,),
            ),
            "db.pool.acquire.duration": (
                True, AggregationTemporality.CUMULATIVE, (2.5,),
            ),
        },
        {
            "db.pool.acquire.count": (
                True, AggregationTemporality.CUMULATIVE, (9,),
            ),
            "db.pool.acquire.duration": (
                True, AggregationTemporality.CUMULATIVE, (2.5,),
            ),
        },
    )


async def test_published_observations_are_detached_from_mutable_metrics():
    manager = _OwnerManager()
    async with _sdk_meter() as (_reader, meter, _provider):
        recording = _RecordingMeter(meter, manager)
        async with common.observe_hasql_metrics(
            manager, sample_interval=60, extra_keys=("overflow",),
            meter=recording,
        ):
            before = await asyncio.to_thread(_observed_values, recording)
            manager.snapshot.pools[0].lag["bytes"] = 999
            manager.snapshot.pools[0].lag["time"] = timedelta(seconds=99)
            manager.snapshot.pools[0].extra["overflow"] = 999
            manager.snapshot.hasql.acquire["primary"] = 999
            manager.snapshot.hasql.acquire_time["primary"] = 999
            manager.snapshot.pools.clear()
            after = await asyncio.to_thread(_observed_values, recording)

    assert after == before


async def test_callback_attribute_mutation_does_not_change_next_collection():
    manager = _OwnerManager()
    async with _sdk_meter() as (_reader, meter, _provider):
        recording = _RecordingMeter(meter, manager)
        async with common.observe_hasql_metrics(
            manager, sample_interval=60, meter=recording,
        ):
            before = await asyncio.to_thread(_observed_values, recording)
            observations = await asyncio.to_thread(_all_observations, recording)
            observations["db.pool.connections.used"][0].attributes["host"] = (
                "exporter-mutated"
            )
            after = await asyncio.to_thread(_observed_values, recording)

    assert after == before


async def test_resampling_removes_missing_pools_lag_extras_and_counter_hosts():
    manager = _OwnerManager()
    async with _sdk_meter() as (_reader, meter, _provider):
        recording = _RecordingMeter(meter, manager)
        async with common.observe_hasql_metrics(
            manager, sample_interval=0.01, extra_keys=("overflow",),
            meter=recording,
        ):
            manager.snapshot = replace(
                manager.snapshot,
                pools=[replace(
                    manager.snapshot.pools[0], host="replacement", role=None,
                    healthy=False, staleness=None, lag={}, extra={},
                    response_time=None,
                )],
            )
            manager.snapshot.hasql.acquire.clear()
            manager.snapshot.hasql.acquire_time.clear()
            await _next_sample(manager)
            values = await asyncio.to_thread(_observed_values, recording)
            manager.snapshot.pools.clear()
            await _next_sample(manager)
            empty = await asyncio.to_thread(_observed_values, recording)

    assert (
        values["db.pool.connections.used"],
        values["db.pool.healthy"],
        values["db.pool.health_check.duration"],
        values["db.pool.stale.status"],
        values["db.pool.stale.lag.bytes"],
        values["db.pool.stale.lag.time"],
        values["db.pool.extra.overflow"],
        values["db.pool.acquire.count"],
        values["db.pool.acquire.duration"],
        empty["db.pool.connections.used"],
    ) == (
        ((2, {"host": "replacement", "role": "unknown"}),),
        ((0, {"host": "replacement", "role": "unknown"}),),
        (), (), (), (), (), (), (), (),
    )


@pytest.mark.parametrize("sample_interval", [0, -1])
async def test_observer_rejects_nonpositive_sample_interval(sample_interval):
    manager = _OwnerManager()
    async with _sdk_meter() as (_reader, meter, _provider):
        with pytest.raises(ValueError):
            async with common.observe_hasql_metrics(
                manager, sample_interval=sample_interval, meter=meter,
            ):
                pass


async def test_sampler_runs_without_workload_and_is_awaited_on_exit():
    manager = _OwnerManager()
    tasks_before = asyncio.all_tasks()
    async with _sdk_meter() as (_reader, meter, _provider):
        async with common.observe_hasql_metrics(
            manager, sample_interval=0.01, meter=meter,
        ):
            await _next_sample(manager)
            await _next_sample(manager)
            sampled_repeatedly = manager.calls >= 3

    assert (sampled_repeatedly, asyncio.all_tasks()) == (True, tasks_before)


async def test_initial_sampling_failure_propagates_without_leaking_task():
    manager = _OwnerManager()
    manager.error = RuntimeError("initial sample failed")
    tasks_before = asyncio.all_tasks()
    async with _sdk_meter() as (_reader, meter, _provider):
        with pytest.raises(RuntimeError, match="initial sample failed"):
            async with common.observe_hasql_metrics(manager, meter=meter):
                pass

    assert asyncio.all_tasks() == tasks_before


async def test_runtime_sampling_failure_logs_clears_snapshot_and_recovers(
    caplog,
):
    manager = _OwnerManager()
    async with _sdk_meter() as (_reader, meter, _provider):
        recording = _RecordingMeter(meter, manager)
        async with common.observe_hasql_metrics(
            manager, sample_interval=0.01, extra_keys=("overflow",),
            meter=recording,
        ):
            manager.error = RuntimeError("sampling probe failed")
            await _next_sample(manager)
            failed = await asyncio.to_thread(_observed_values, recording)
            manager.error = None
            await _next_sample(manager)
            recovered = await asyncio.to_thread(_observed_values, recording)

    assert (
        set(failed.values()),
        recovered["db.pool.acquire.count"],
        "sampling probe failed" in caplog.text,
    ) == ({()}, ((7, {"host": "primary"}),), True)


async def test_cancellation_awaits_sampler_shutdown():
    observe_metrics = common.observe_hasql_metrics
    manager = _OwnerManager()
    entered = asyncio.Event()
    tasks_before = asyncio.all_tasks()
    async with _sdk_meter() as (_reader, meter, _provider):
        async def observe():
            async with observe_metrics(
                manager, sample_interval=0.01, meter=meter,
            ):
                entered.set()
                await asyncio.Future()

        task = asyncio.create_task(observe())
        await asyncio.wait_for(entered.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert asyncio.all_tasks() == tasks_before


async def _wait_thread_event(event):
    signalled = await asyncio.to_thread(event.wait, 2)
    if not signalled:
        raise TimeoutError("worker did not signal within 2 seconds")


@asynccontextmanager
async def _paused_callback(callback):
    started = threading.Event()
    resume = threading.Event()

    def collect():
        observations = iter(callback(CallbackOptions()))
        first = next(observations)
        started.set()
        if not resume.wait(2):
            raise TimeoutError("callback was not resumed within 2 seconds")
        return (first, *observations)

    task = asyncio.create_task(asyncio.to_thread(collect))
    try:
        await _wait_thread_event(started)
        yield task
    finally:
        resume.set()
        await task


async def test_callback_keeps_one_snapshot_without_holding_lock_across_yield():
    manager = _OwnerManager()
    manager.snapshot.pools.append(replace(
        manager.snapshot.pools[0], host="second", used=3,
    ))
    async with _sdk_meter() as (_reader, meter, _provider):
        recording = _RecordingMeter(meter, manager)
        async with common.observe_hasql_metrics(
            manager, sample_interval=0.01, meter=recording,
        ):
            callback, = recording.callbacks["db.pool.connections.used"]
            async with _paused_callback(callback) as task:
                manager.snapshot.pools[:] = [
                    replace(manager.snapshot.pools[0], used=9),
                ]
                await _next_sample(manager)
            old = tuple(item.value for item in task.result())
            new = await asyncio.to_thread(_observed_values, recording)

    assert (old, new["db.pool.connections.used"]) == (
        (2, 3),
        ((9, {"host": "primary", "role": "replica", "staleness": "stale"}),),
    )


class _LocalExporter(MetricExporter):
    """Real reader's bounded, network-free export boundary."""

    def __init__(self):
        super().__init__()
        self.exported = threading.Event()
        self.stopped = threading.Event()
        self.threads = []

    def export(self, metrics_data, timeout_millis=10_000, **kwargs):
        self.threads.append(threading.get_ident())
        self.exported.set()
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis=10_000):
        return True

    def shutdown(self, timeout_millis=30_000, **kwargs):
        self.stopped.set()


async def test_real_periodic_reader_collects_off_loop_and_shuts_down():
    observe_metrics = common.observe_hasql_metrics
    manager = _OwnerManager()
    exporter = _LocalExporter()
    reader = PeriodicExportingMetricReader(
        exporter, export_interval_millis=20, export_timeout_millis=1000,
    )
    async with _sdk_meter(reader) as (_reader, meter, _provider):
        async with observe_metrics(manager, sample_interval=60, meter=meter):
            await _wait_thread_event(exporter.exported)

    assert (
        manager.calls, manager.foreign_threads,
        manager.owner_thread in exporter.threads,
        exporter.stopped.is_set(),
    ) == (1, [], False, True)


_EXAMPLES = ("aiopg", "aiopg_sa", "asyncpg", "asyncsqlalchemy", "psycopg3")
_PROJECT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("adapter", _EXAMPLES)
def test_example_supports_module_execution_help(adapter):
    process = subprocess.run(
        [sys.executable, "-m", f"example.otlp.{adapter}", "--help"],
        cwd=_PROJECT, text=True, capture_output=True, timeout=10,
    )

    assert (process.returncode, "--dsn" in process.stdout, process.stderr) == (
        0, True, "",
    )


class _ExampleFailure(RuntimeError):
    pass


class _ExampleLifecycle:
    """Controlled ownership boundaries; no driver or query emulation."""

    def __init__(self, phase):
        self.phase = phase
        self.loop = asyncio.get_running_loop()
        self.owner_thread = threading.get_ident()
        self.events = []
        self.observing = False
        self.workload_entered = asyncio.Event()

    async def ready(self):
        if self.phase == "ready":
            raise _ExampleFailure("ready")

    async def close(self):
        self.events.append(("close", not self.observing))
        if self.phase == "close":
            raise _ExampleFailure("close")

    def shutdown(self):
        heartbeat = threading.Event()
        self.loop.call_soon_threadsafe(heartbeat.set)
        off_loop = threading.get_ident() != self.owner_thread
        loop_responsive = off_loop and heartbeat.wait(2)
        self.events.append(("shutdown", loop_responsive))

    @asynccontextmanager
    async def acquire_master(self):
        self.workload_entered.set()
        if self.phase == "cancellation":
            await asyncio.Future()
        raise _ExampleFailure("workload")
        yield  # pragma: no cover -- async context manager never returns a conn

    @asynccontextmanager
    async def observe(self, *_args, **_kwargs):
        if self.phase == "registration":
            raise _ExampleFailure("registration")
        self.observing = True
        try:
            yield
        finally:
            self.observing = False

    def legacy_register(self, *_args, **_kwargs):
        # Transitional seam: proves current main() cleanup failures before the
        # examples switch from register_hasql_metrics to observe_hasql_metrics.
        if self.phase == "registration":
            raise _ExampleFailure("registration")
        self.observing = True


def _example_main(monkeypatch, adapter, lifecycle):
    # Isolate lifecycle from the separately asserted module-import defect.
    # No sys.path edits or real OpenTelemetry global provider resets.
    monkeypatch.setitem(sys.modules, "common", common)
    module = importlib.import_module(f"example.otlp.{adapter}")
    monkeypatch.setattr(module, "PoolManager", lambda *a, **kw: lifecycle)
    monkeypatch.setattr(module, "setup_meter_provider", lambda **kw: lifecycle)
    monkeypatch.setattr(
        module, "observe_hasql_metrics", lifecycle.observe, raising=False,
    )
    monkeypatch.setattr(
        module, "register_hasql_metrics", lifecycle.legacy_register,
        raising=False,
    )
    monkeypatch.setattr(
        module, "register_extra_gauges", lambda *a, **kw: None, raising=False,
    )
    monkeypatch.setattr(
        sys, "argv", [f"example.otlp.{adapter}", "--dsn", "unused-dsn"],
    )
    return module.main


@pytest.mark.parametrize("adapter", _EXAMPLES)
@pytest.mark.parametrize(
    "phase", ["ready", "registration", "workload", "close"],
)
async def test_example_failure_closes_pool_before_off_loop_provider_shutdown(
    adapter, phase, monkeypatch,
):
    lifecycle = _ExampleLifecycle(phase)
    main = _example_main(monkeypatch, adapter, lifecycle)

    with pytest.raises(_ExampleFailure, match=phase):
        await main()

    assert lifecycle.events == [("close", True), ("shutdown", True)]


@pytest.mark.parametrize("adapter", _EXAMPLES)
async def test_example_cancellation_stops_observation_then_closes_resources(
    adapter, monkeypatch,
):
    lifecycle = _ExampleLifecycle("cancellation")
    main = _example_main(monkeypatch, adapter, lifecycle)
    task = asyncio.create_task(main())

    await asyncio.wait_for(lifecycle.workload_entered.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert lifecycle.events == [("close", True), ("shutdown", True)]


async def test_registration_failure_does_not_leave_sampler_running(monkeypatch):
    manager = _OwnerManager()
    tasks_before = asyncio.all_tasks()
    async with _sdk_meter() as (_reader, meter, _provider):
        def fail_registration(*args, **kwargs):
            raise RuntimeError("instrument registration failed")

        monkeypatch.setattr(meter, "create_observable_gauge", fail_registration)
        with pytest.raises(
            RuntimeError, match="instrument registration failed",
        ):
            async with common.observe_hasql_metrics(manager, meter=meter):
                pass

    assert asyncio.all_tasks() == tasks_before


async def test_observer_body_exception_awaits_sampler_shutdown():
    manager = _OwnerManager()
    tasks_before = asyncio.all_tasks()
    async with _sdk_meter() as (_reader, meter, _provider):
        with pytest.raises(RuntimeError, match="body failed"):
            async with common.observe_hasql_metrics(manager, meter=meter):
                raise RuntimeError("body failed")

    assert asyncio.all_tasks() == tasks_before
