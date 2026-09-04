import asyncio
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from enum import Enum

import pytest

from hasql.health import PoolHealthMonitor
from hasql.metrics import HasqlGauges, PoolMetrics, PoolRole, PoolStaleness
from hasql.pool_state import PoolState
from hasql.staleness import (
    BaseStalenessChecker,
    BytesStalenessChecker,
    CheckContext,
    StalenessCheckResult,
    StalenessPolicy,
    TimeStalenessChecker,
)
from hasql.utils import Dsn
from tests.mocks.pool_manager import TestDriver, TestPool


CONCURRENT_TEST_TIMEOUT = 1.0


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


class FailingChecker(BaseStalenessChecker):
    async def check(self, ctx):
        raise RuntimeError("staleness check failed")


class BlockingRoleDriver(TestDriver):
    def __init__(self):
        self.block_role_probe = False
        self.role_probe_started = asyncio.Event()
        self.release_role_probe = asyncio.Event()

    async def is_master(self, connection):
        if self.block_role_probe:
            self.role_probe_started.set()
            await asyncio.wait_for(
                self.release_role_probe.wait(),
                timeout=CONCURRENT_TEST_TIMEOUT,
            )
        return await super().is_master(connection)


class BlockingChecker(BaseStalenessChecker):
    def __init__(self, result):
        self.result = result
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def check(self, ctx):
        self.started.set()
        await asyncio.wait_for(
            self.release.wait(), timeout=CONCURRENT_TEST_TIMEOUT,
        )
        return self.result


class BlockingFailingChecker(BaseStalenessChecker):
    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def check(self, ctx):
        self.started.set()
        await asyncio.wait_for(
            self.release.wait(), timeout=CONCURRENT_TEST_TIMEOUT,
        )
        raise RuntimeError("staleness check failed")


class BlockingMasterCollector(BaseStalenessChecker):
    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def check(self, ctx):
        return StalenessCheckResult(is_stale=False, lag={})

    async def collect_master_state(self, ctx):
        self.started.set()
        await asyncio.wait_for(
            self.release.wait(), timeout=CONCURRENT_TEST_TIMEOUT,
        )


class BlockingFailingMasterCollector(BlockingMasterCollector):
    async def collect_master_state(self, ctx):
        self.started.set()
        await asyncio.wait_for(
            self.release.wait(), timeout=CONCURRENT_TEST_TIMEOUT,
        )
        raise RuntimeError("master state collection failed")


async def make_stopped_monitor(pool_state):
    monitor = PoolHealthMonitor(pool_state, 1, 1, lambda: True)
    await monitor.stop()
    return monitor


async def prepare_replica_state(
    monitor, pool, dsn, connection, policy, initial_checker,
):
    if initial_checker is not None:
        policy._checker = initial_checker
        await monitor._full_pool_check(pool, dsn, connection)


@asynccontextmanager
async def blocked_check(check, started, release):
    check_task = asyncio.create_task(check)
    try:
        await asyncio.wait_for(
            started.wait(), timeout=CONCURRENT_TEST_TIMEOUT,
        )
        yield
        release.set()
        await asyncio.wait_for(
            asyncio.shield(check_task), timeout=CONCURRENT_TEST_TIMEOUT,
        )
    finally:
        release.set()
        if not check_task.done():
            check_task.cancel()
        await asyncio.gather(check_task, return_exceptions=True)


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


@pytest.mark.asyncio
async def test_fresh_replica_checker_error_removes_all_pool_state():
    dsn = Dsn.parse("postgresql://test:test@replica:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    pool_state = PoolState([dsn], TestDriver(), 10, staleness=policy)
    monitor = PoolHealthMonitor(pool_state, 1, 1, lambda: True)
    await monitor.stop()
    await pool_state.refresh_pool_role(pool, dsn, connection)
    await pool_state.check_replica_staleness(pool, dsn, connection)
    policy._checker = FailingChecker()

    await monitor._full_pool_check(pool, dsn, connection)

    assert (
        pool_state.pool_is_master(pool),
        pool_state.pool_is_replica(pool),
        pool_state.pool_is_stale(pool),
        pool_state.get_last_check_result(pool),
        pool in policy._last_fresh_at,
    ) == (False, False, False, None, False)


@pytest.mark.asyncio
async def test_clear_sets_clears_staleness_tracking_and_cached_results():
    dsn = Dsn.parse("postgresql://test:test@replica:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    pool_state = PoolState([dsn], TestDriver(), 10, staleness=policy)
    await pool_state.refresh_pool_role(pool, dsn, connection)
    await pool_state.check_replica_staleness(pool, dsn, connection)

    pool_state.clear_sets()

    assert (
        pool_state.get_last_check_result(pool),
        pool in policy._last_fresh_at,
    ) == (None, False)


@pytest.mark.asyncio
async def test_replica_retains_confirmed_state_while_role_probe_blocks():
    dsn = Dsn.parse("postgresql://test:test@replica:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    driver = BlockingRoleDriver()
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    pool_state = PoolState([dsn], driver, 10, staleness=policy)
    monitor = await make_stopped_monitor(pool_state)
    await monitor._full_pool_check(pool, dsn, connection)
    pool.set_master(True)
    driver.block_role_probe = True

    async with blocked_check(
        monitor._full_pool_check(pool, dsn, connection),
        driver.role_probe_started,
        driver.release_role_probe,
    ):
        state_during_probe = (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        )

    assert (
        state_during_probe,
        (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        ),
    ) == (
        (False, True, False),
        (True, False, False),
    )


@pytest.mark.parametrize(
    ("initial_checker", "expected_during_check"),
    [
        (AlwaysFreshChecker(), (False, True, False)),
        (AlwaysStaleChecker(), (False, False, True)),
        (None, (False, False, False)),
    ],
    ids=("fresh", "stale", "unavailable"),
)
@pytest.mark.asyncio
async def test_replica_retains_confirmed_state_while_staleness_check_blocks(
    initial_checker, expected_during_check,
):
    dsn = Dsn.parse("postgresql://test:test@replica:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    checker = BlockingChecker(
        StalenessCheckResult(is_stale=False, lag={}),
    )
    policy = StalenessPolicy(checker=checker)
    pool_state = PoolState([dsn], TestDriver(), 10, staleness=policy)
    monitor = await make_stopped_monitor(pool_state)
    await prepare_replica_state(
        monitor, pool, dsn, connection, policy, initial_checker,
    )
    policy._checker = checker

    async with blocked_check(
        monitor._full_pool_check(pool, dsn, connection),
        checker.started,
        checker.release,
    ):
        state_during_check = (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        )

    assert state_during_check == expected_during_check


@pytest.mark.parametrize(
    ("result", "expected_state"),
    [
        (
            StalenessCheckResult(is_stale=False, lag={}),
            (False, True, False),
        ),
        (
            StalenessCheckResult(is_stale=True, lag={}),
            (False, False, True),
        ),
    ],
    ids=("fresh", "stale"),
)
@pytest.mark.asyncio
async def test_new_replica_becomes_ready_after_staleness_check_succeeds(
    result, expected_state,
):
    dsn = Dsn.parse("postgresql://test:test@replica:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    checker = BlockingChecker(result)
    policy = StalenessPolicy(checker=checker)
    pool_state = PoolState([dsn], TestDriver(), 10, staleness=policy)
    monitor = await make_stopped_monitor(pool_state)

    async with blocked_check(
        monitor._full_pool_check(pool, dsn, connection),
        checker.started,
        checker.release,
    ):
        ready_during_check = pool_state._dsn_ready_event[dsn].is_set()

    assert (
        ready_during_check,
        pool_state._dsn_ready_event[dsn].is_set(),
        (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        ),
    ) == (False, True, expected_state)


@pytest.mark.asyncio
async def test_new_replica_remains_unready_after_staleness_check_fails():
    dsn = Dsn.parse("postgresql://test:test@replica:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    checker = BlockingFailingChecker()
    policy = StalenessPolicy(checker=checker)
    pool_state = PoolState([dsn], TestDriver(), 10, staleness=policy)
    monitor = await make_stopped_monitor(pool_state)

    async with blocked_check(
        monitor._full_pool_check(pool, dsn, connection),
        checker.started,
        checker.release,
    ):
        ready_during_check = pool_state._dsn_ready_event[dsn].is_set()

    assert (
        ready_during_check,
        pool_state._dsn_ready_event[dsn].is_set(),
        pool_state.pool_is_master(pool),
        pool_state.pool_is_replica(pool),
        pool_state.pool_is_stale(pool),
        pool_state.get_last_check_result(pool),
    ) == (False, False, False, False, False, None)


@pytest.mark.asyncio
async def test_ready_replica_keeps_readiness_after_staleness_check_fails():
    dsn = Dsn.parse("postgresql://test:test@replica:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    checker = BlockingFailingChecker()
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    pool_state = PoolState([dsn], TestDriver(), 10, staleness=policy)
    monitor = await make_stopped_monitor(pool_state)
    await monitor._full_pool_check(pool, dsn, connection)
    policy._checker = checker

    async with blocked_check(
        monitor._full_pool_check(pool, dsn, connection),
        checker.started,
        checker.release,
    ):
        state_during_check = (
            pool_state._dsn_ready_event[dsn].is_set(),
            pool_state.pool_is_replica(pool),
        )

    assert (
        state_during_check,
        pool_state._dsn_ready_event[dsn].is_set(),
        pool_state.pool_is_master(pool),
        pool_state.pool_is_replica(pool),
        pool_state.pool_is_stale(pool),
        pool_state.get_last_check_result(pool),
    ) == ((True, True), True, False, False, False, None)


@pytest.mark.asyncio
async def test_master_becomes_unavailable_until_replica_staleness_is_known():
    dsn = Dsn.parse("postgresql://test:test@master:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    checker = BlockingChecker(
        StalenessCheckResult(is_stale=False, lag={}),
    )
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    pool_state = PoolState([dsn], TestDriver(), 10, staleness=policy)
    monitor = await make_stopped_monitor(pool_state)
    await monitor._full_pool_check(pool, dsn, connection)
    policy._checker = checker
    pool.set_master(False)

    async with blocked_check(
        monitor._full_pool_check(pool, dsn, connection),
        checker.started,
        checker.release,
    ):
        state_during_check = (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        )

    assert (
        state_during_check,
        (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        ),
    ) == (
        (False, False, False),
        (False, True, False),
    )


@pytest.mark.parametrize(
    "initial_checker",
    [AlwaysFreshChecker(), AlwaysStaleChecker()],
    ids=("replica", "stale"),
)
@pytest.mark.asyncio
async def test_read_pool_becomes_master_after_master_state_is_collected(
    initial_checker,
):
    dsn = Dsn.parse("postgresql://test:test@replica:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    collector = BlockingMasterCollector()
    policy = StalenessPolicy(checker=initial_checker)
    pool_state = PoolState([dsn], TestDriver(), 10, staleness=policy)
    monitor = await make_stopped_monitor(pool_state)
    await monitor._full_pool_check(pool, dsn, connection)
    policy._checker = collector
    pool.set_master(True)

    async with blocked_check(
        monitor._full_pool_check(pool, dsn, connection),
        collector.started,
        collector.release,
    ):
        state_during_collection = (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        )

    assert (
        state_during_collection,
        (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        ),
    ) == (
        (False, False, False),
        (True, False, False),
    )


@pytest.mark.asyncio
async def test_existing_master_is_removed_after_collection_fails():
    dsn = Dsn.parse("postgresql://test:test@master:5432/test")
    pool = TestPool(str(dsn))
    connection = pool.connections[0]
    collector = BlockingFailingMasterCollector()
    policy = StalenessPolicy(checker=AlwaysFreshChecker())
    pool_state = PoolState([dsn], TestDriver(), 10, staleness=policy)
    monitor = await make_stopped_monitor(pool_state)
    await monitor._full_pool_check(pool, dsn, connection)
    policy._checker = collector

    async with blocked_check(
        monitor._full_pool_check(pool, dsn, connection),
        collector.started,
        collector.release,
    ):
        state_during_collection = (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        )

    assert (
        state_during_collection,
        (
            pool_state.pool_is_master(pool),
            pool_state.pool_is_replica(pool),
            pool_state.pool_is_stale(pool),
        ),
        pool_state.get_last_check_result(pool),
        pool_state._dsn_ready_event[dsn].is_set(),
    ) == (
        (True, False, False),
        (False, False, False),
        None,
        True,
    )
