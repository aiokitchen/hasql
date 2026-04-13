import time
from datetime import timedelta
from enum import Enum

import pytest

from hasql.metrics import HasqlGauges, PoolMetrics, PoolRole, PoolStaleness
from hasql.staleness import (
    BaseStalenessChecker,
    BytesStalenessChecker,
    CheckContext,
    StalenessCheckResult,
    StalenessPolicy,
    TimeStalenessChecker,
)


class MockDriver:
    async def fetch_scalar(self, connection, query):
        return f"result:{query}"


class MockConnection:
    pass


def test_pool_role_values():
    assert PoolRole.MASTER == "master"
    assert PoolRole.REPLICA == "replica"
    assert isinstance(PoolRole.MASTER, str)
    assert isinstance(PoolRole.MASTER, Enum)


def test_pool_staleness_values():
    assert PoolStaleness.FRESH == "fresh"
    assert PoolStaleness.STALE == "stale"
    assert isinstance(PoolStaleness.FRESH, str)
    assert isinstance(PoolStaleness.FRESH, Enum)


def test_pool_metrics_with_staleness_fields():
    pm = PoolMetrics(
        host="localhost",
        role=PoolRole.REPLICA,
        healthy=True,
        min=1,
        max=10,
        idle=5,
        used=4,
        response_time=0.01,
        in_flight=1,
        staleness=PoolStaleness.FRESH,
        lag={"time": 1.5},
    )
    assert pm.staleness == PoolStaleness.FRESH
    assert pm.lag == {"time": 1.5}


def test_pool_metrics_staleness_none_for_master():
    pm = PoolMetrics(
        host="localhost",
        role=PoolRole.MASTER,
        healthy=True,
        min=1,
        max=10,
        idle=5,
        used=4,
        response_time=0.01,
        in_flight=0,
        staleness=None,
        lag={},
    )
    assert pm.staleness is None
    assert pm.lag == {}


def test_hasql_gauges_with_stale_and_unavailable():
    g = HasqlGauges(
        master_count=1,
        replica_count=2,
        available_count=4,
        active_connections=3,
        closing=False,
        closed=False,
        stale_count=1,
        unavailable_count=0,
    )
    assert g.stale_count == 1
    assert g.unavailable_count == 0


def test_staleness_check_result_creation():
    result = StalenessCheckResult(is_stale=True, lag={"time": 5.0})
    assert result.is_stale is True
    assert result.lag == {"time": 5.0}


def test_staleness_check_result_is_frozen():
    result = StalenessCheckResult(is_stale=False, lag={})
    with pytest.raises(AttributeError):
        result.is_stale = True  # type: ignore[misc]


@pytest.mark.asyncio
async def test_check_context_fetch_scalar():
    driver = MockDriver()
    conn = MockConnection()
    ctx = CheckContext(connection=conn, driver=driver)
    result = await ctx.fetch_scalar("SELECT 1")
    assert result == "result:SELECT 1"


@pytest.mark.asyncio
async def test_base_staleness_checker_collect_master_state_is_noop():
    """Default collect_master_state does nothing."""
    class NoopChecker(BaseStalenessChecker):
        async def check(self, ctx):
            return StalenessCheckResult(is_stale=False, lag={})

    checker = NoopChecker()
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())
    # Should not raise
    await checker.collect_master_state(ctx)


class TimeMockDriver:
    def __init__(self, lag_interval):
        self._lag_interval = lag_interval

    async def fetch_scalar(self, connection, query):
        return self._lag_interval


@pytest.mark.asyncio
async def test_time_staleness_checker_fresh():
    driver = TimeMockDriver(lag_interval=timedelta(seconds=5))
    ctx = CheckContext(connection=MockConnection(), driver=driver)
    checker = TimeStalenessChecker(max_lag=timedelta(seconds=10))
    result = await checker.check(ctx)
    assert result.is_stale is False
    assert result.lag == {"time": timedelta(seconds=5)}


@pytest.mark.asyncio
async def test_time_staleness_checker_stale():
    driver = TimeMockDriver(lag_interval=timedelta(seconds=15))
    ctx = CheckContext(connection=MockConnection(), driver=driver)
    checker = TimeStalenessChecker(max_lag=timedelta(seconds=10))
    result = await checker.check(ctx)
    assert result.is_stale is True
    assert result.lag == {"time": timedelta(seconds=15)}


@pytest.mark.asyncio
async def test_time_staleness_checker_null_replay_timestamp():
    driver = TimeMockDriver(lag_interval=None)
    ctx = CheckContext(connection=MockConnection(), driver=driver)
    checker = TimeStalenessChecker(max_lag=timedelta(seconds=10))
    result = await checker.check(ctx)
    assert result.is_stale is True
    assert result.lag == {}


@pytest.mark.asyncio
async def test_time_staleness_checker_exact_threshold():
    driver = TimeMockDriver(lag_interval=timedelta(seconds=10))
    ctx = CheckContext(connection=MockConnection(), driver=driver)
    checker = TimeStalenessChecker(max_lag=timedelta(seconds=10))
    result = await checker.check(ctx)
    assert result.is_stale is False


class BytesMockDriver:
    def __init__(self, lag_bytes=None, master_lsn=None):
        self._lag_bytes = lag_bytes
        self._master_lsn = master_lsn

    async def fetch_scalar(self, connection, query):
        if "pg_current_wal_lsn" in query:
            return self._master_lsn
        if "pg_wal_lsn_diff" in query:
            return self._lag_bytes
        return None


@pytest.mark.asyncio
async def test_bytes_staleness_checker_fresh():
    driver = BytesMockDriver(lag_bytes=1000)
    master_driver = BytesMockDriver(master_lsn="0/1000000")
    ctx_master = CheckContext(
        connection=MockConnection(), driver=master_driver,
    )
    ctx_replica = CheckContext(connection=MockConnection(), driver=driver)

    checker = BytesStalenessChecker(max_lag_bytes=1024 * 1024)
    await checker.collect_master_state(ctx_master)
    result = await checker.check(ctx_replica)
    assert result.is_stale is False
    assert result.lag == {"bytes": 1000}


@pytest.mark.asyncio
async def test_bytes_staleness_checker_stale():
    driver = BytesMockDriver(lag_bytes=2 * 1024 * 1024)
    master_driver = BytesMockDriver(master_lsn="0/2000000")
    ctx_master = CheckContext(
        connection=MockConnection(), driver=master_driver,
    )
    ctx_replica = CheckContext(connection=MockConnection(), driver=driver)

    checker = BytesStalenessChecker(max_lag_bytes=1024 * 1024)
    await checker.collect_master_state(ctx_master)
    result = await checker.check(ctx_replica)
    assert result.is_stale is True
    assert result.lag == {"bytes": 2 * 1024 * 1024}


@pytest.mark.asyncio
async def test_bytes_staleness_checker_no_master_lsn():
    driver = BytesMockDriver(lag_bytes=999)
    ctx = CheckContext(connection=MockConnection(), driver=driver)

    checker = BytesStalenessChecker(max_lag_bytes=100)
    result = await checker.check(ctx)
    assert result.is_stale is False
    assert result.lag == {}


@pytest.mark.asyncio
async def test_bytes_staleness_checker_stale_master_lsn():
    """If cached master LSN is too old, assume fresh."""
    driver = BytesMockDriver(lag_bytes=2 * 1024 * 1024)
    master_driver = BytesMockDriver(master_lsn="0/3000000")
    ctx_master = CheckContext(
        connection=MockConnection(), driver=master_driver,
    )
    ctx_replica = CheckContext(connection=MockConnection(), driver=driver)

    checker = BytesStalenessChecker(
        max_lag_bytes=1024 * 1024,
        max_master_lsn_age=timedelta(seconds=2),
    )
    await checker.collect_master_state(ctx_master)

    # Simulate time passing beyond max_master_lsn_age
    checker._master_lsn_updated_at -= 3.0

    result = await checker.check(ctx_replica)
    assert result.is_stale is False
    assert result.lag == {}


class AlwaysFreshChecker(BaseStalenessChecker):
    async def check(self, ctx):
        return StalenessCheckResult(
            is_stale=False, lag={"time": timedelta(seconds=1)},
        )


class AlwaysStaleChecker(BaseStalenessChecker):
    async def check(self, ctx):
        return StalenessCheckResult(
            is_stale=True, lag={"time": timedelta(seconds=20)},
        )


@pytest.mark.asyncio
async def test_staleness_policy_fresh():
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())
    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is False


@pytest.mark.asyncio
async def test_staleness_policy_stale():
    policy = StalenessPolicy(checker=AlwaysStaleChecker())
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())
    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is True


@pytest.mark.asyncio
async def test_staleness_policy_grace_period():
    """Pool stays fresh during grace period even when checker says stale."""
    policy = StalenessPolicy(
        checker=AlwaysFreshChecker(),
        grace_period=timedelta(seconds=30),
    )
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())

    # First check: fresh — records last_fresh_at
    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is False

    # Switch to stale checker
    policy._checker = AlwaysStaleChecker()

    # Second check: checker says stale but within grace period
    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is False
    assert result.lag == {"time": timedelta(seconds=20)}


@pytest.mark.asyncio
async def test_staleness_policy_grace_period_expired():
    """Pool becomes stale after grace period expires."""
    policy = StalenessPolicy(
        checker=AlwaysStaleChecker(),
        grace_period=timedelta(seconds=1),
    )
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())

    # Manually set last_fresh_at in the past
    policy._last_fresh_at["pool1"] = time.monotonic() - 2.0

    result = await policy.check(pool="pool1", ctx=ctx)
    assert result.is_stale is True


@pytest.mark.asyncio
async def test_staleness_policy_remove_pool():
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    ctx = CheckContext(connection=MockConnection(), driver=MockDriver())

    await policy.check(pool="pool1", ctx=ctx)
    assert "pool1" in policy._last_fresh_at

    policy.remove_pool("pool1")
    assert "pool1" not in policy._last_fresh_at


@pytest.mark.asyncio
async def test_staleness_policy_remove_pool_not_tracked():
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    # Should not raise
    policy.remove_pool("nonexistent")
