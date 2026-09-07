"""Test that all old import paths still work after the split."""

from importlib import import_module

import pytest

from hasql.metrics import HasqlGauges, PoolMetrics, PoolRole


@pytest.mark.parametrize(
    "driver_name",
    (
        "aiopg",
        "aiopg_sa",
        "asyncpg",
        "asyncpgsa",
        "asyncsqlalchemy",
        "psycopg3",
    ),
)
def test_legacy_driver_module_reexports_pool_manager(driver_name):
    if driver_name == "asyncpgsa":
        pytest.importorskip("asyncpgsa")

    legacy = import_module(f"hasql.{driver_name}")
    canonical = import_module(f"hasql.driver.{driver_name}")

    assert legacy.PoolManager is canonical.PoolManager


def test_pool_metrics_tenth_positional_argument_remains_extra():
    extra = {"driver": "value"}

    metrics = PoolMetrics(
        "localhost",
        PoolRole.REPLICA,
        True,
        1,
        10,
        5,
        4,
        0.01,
        1,
        extra,
    )

    assert (metrics.extra, metrics.staleness, metrics.lag) == (
        extra,
        None,
        {},
    )


def test_hasql_gauges_seventh_positional_argument_remains_unavailable_count():
    gauges = HasqlGauges(1, 2, 3, 4, False, False, 5)

    assert (gauges.unavailable_count, gauges.stale_count) == (5, 0)


def test_asyncsqlalchemy_reexports_sessionmaker():
    from hasql.asyncsqlalchemy import async_sessionmaker
    from hasql.driver.asyncsqlalchemy import async_sessionmaker as canonical

    assert async_sessionmaker is canonical



def test_base_exports_pool_manager():
    from hasql.base import BasePoolManager
    from hasql.pool_manager import BasePoolManager as Direct

    assert BasePoolManager is Direct


def test_base_exports_abstract_balancer_policy():
    from hasql.balancer_policy import AbstractBalancerPolicy as Direct
    from hasql.base import AbstractBalancerPolicy

    assert AbstractBalancerPolicy is Direct


def test_base_exports_timeout_acquire_context():
    from hasql.acquire import TimeoutAcquireContext as Direct
    from hasql.base import TimeoutAcquireContext

    assert TimeoutAcquireContext is Direct


def test_base_exports_pool_acquire_context():
    from hasql.acquire import PoolAcquireContext as Direct
    from hasql.base import PoolAcquireContext

    assert PoolAcquireContext is Direct


def test_psycopg3_exports_pool_acquire_context_alias():
    from hasql.psycopg3 import PoolAcquireContext
    from hasql.psycopg3 import Psycopg3AcquireContext

    assert PoolAcquireContext is Psycopg3AcquireContext
