import asyncio
from unittest.mock import AsyncMock

import pytest

from hasql.acquire import PoolAcquireContext
from hasql.health import PoolHealthMonitor
from hasql.metrics import CalculateMetrics
from hasql.utils import Dsn
from tests.mocks import TestPoolManager
from tests.mocks.pool_manager import TestDriver, TestPool


class DelayedBalancer:
    def __init__(self, pool, delay: float):
        self.pool = pool
        self.delay = delay

    async def get_pool(self, **kwargs):
        await asyncio.sleep(self.delay)
        return self.pool


class SlowAcquire:
    def __init__(self, delay: float):
        self.delay = delay

    async def wait(self):
        await asyncio.sleep(self.delay)
        return object()

    def __await__(self):
        return self.wait().__await__()


class _TimeoutSlowAcquire:
    def __init__(self, slow_acquire: SlowAcquire, timeout: float):
        self._slow_acquire = slow_acquire
        self._timeout = timeout

    def __await__(self):
        return asyncio.wait_for(
            self._slow_acquire.wait(),
            timeout=self._timeout,
        ).__await__()


class _PoolStateProxy:
    def __init__(self, parent):
        self._parent = parent

    def acquire_from_pool(self, pool, *, timeout=None, **kwargs):
        self._parent.acquire_timeout = timeout
        slow = SlowAcquire(self._parent.acquire_delay)
        if timeout is not None:
            return _TimeoutSlowAcquire(slow, timeout)
        return slow

    def host(self, pool):
        return "test-host:5432"


class RecordingPoolManager:
    def __init__(self, pool_delay: float, acquire_delay: float):
        self.pool = object()
        self._balancer = DelayedBalancer(self.pool, delay=pool_delay)
        self.acquire_delay = acquire_delay
        self.acquire_timeout = None
        self._pool_state = _PoolStateProxy(self)

    def _register_connection(self, connection, pool):
        pass


class OneConnectionTestDriver(TestDriver):
    async def pool_factory(self, dsn, **kwargs):
        return TestPool(str(dsn), maxsize=1)


class OneConnectionPoolManager(TestPoolManager):
    def __init__(self, dsn, **kwargs):
        super().__init__(dsn, **kwargs)
        self._pool_state._driver = OneConnectionTestDriver()


class TimeoutPoolState:
    def __init__(self):
        self.dsn = [Dsn.parse("postgresql://test:test@master:5432/test")]
        self.removed = False
        self.checked = False

    async def refresh_pool_role(self, pool, dsn, connection):
        await asyncio.Event().wait()

    def pool_is_stale(self, pool):
        return False

    def remove_pool_from_all_sets(self, pool, dsn):
        self.removed = True

    async def notify_pool_checked(self, dsn):
        self.checked = True


async def wait_until(predicate, timeout: float = 1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0.01)


async def test_acquire_timeout_uses_shared_budget():
    recording = RecordingPoolManager(pool_delay=0.05, acquire_delay=1.0)
    context = PoolAcquireContext(
        pool_state=recording._pool_state,
        balancer=recording._balancer,
        register_connection=recording._register_connection,
        unregister_connection=lambda conn: None,
        read_only=False,
        fallback_master=False,
        master_as_replica_weight=None,
        timeout=0.1,
        metrics=CalculateMetrics(),
    )

    start = asyncio.get_running_loop().time()
    with pytest.raises(asyncio.TimeoutError):
        await context

    elapsed = asyncio.get_running_loop().time() - start
    assert elapsed < 0.2
    assert recording.acquire_timeout is not None
    assert 0 < recording.acquire_timeout < 0.1


async def test_refresh_timeout_removes_pool_from_available_set():
    pool_state = TimeoutPoolState()
    monitor = PoolHealthMonitor(
        pool_state,
        refresh_delay=0,
        refresh_timeout=0.02,
        closing_getter=lambda: pool_state.checked,
    )
    await monitor.stop()

    await asyncio.wait_for(
        monitor._periodic_pool_check(
            object(), pool_state.dsn[0], object(),
        ),
        timeout=0.2,
    )

    assert pool_state.removed


async def test_close_preserves_cancellation_during_sys_connection_release():
    pool_manager = TestPoolManager(
        "postgresql://test:test@master,replica1,replica2/test",
        refresh_timeout=0.2,
        refresh_delay=0.05,
    )
    try:
        await pool_manager._pool_state.ready()
        refresh_tasks = list(pool_manager._health.tasks)
        pool_manager._pool_state.release_to_pool = AsyncMock(
            side_effect=asyncio.CancelledError(),
        )

        await pool_manager.close()

        assert pool_manager._closed
        assert pool_manager._pool_state.release_to_pool.await_count > 0
        assert all(task.done() for task in refresh_tasks)
    finally:
        if not pool_manager._closed:
            await pool_manager.close()
