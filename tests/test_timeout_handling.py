import asyncio
from unittest.mock import AsyncMock

import pytest

from hasql.base import PoolAcquireContext
from hasql.metrics import CalculateMetrics
from tests.mocks import TestPoolManager
from tests.mocks.pool_manager import TestPool


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


class RecordingPoolManager:
    def __init__(self, pool_delay: float, acquire_delay: float):
        self.pool = object()
        self.balancer = DelayedBalancer(self.pool, delay=pool_delay)
        self.acquire_delay = acquire_delay
        self.acquire_kwargs = None

    def _prepare_acquire_kwargs(self, kwargs: dict, timeout):
        prepared_kwargs = dict(kwargs)
        prepared_kwargs["timeout"] = timeout
        return prepared_kwargs

    def acquire_from_pool(self, pool, **kwargs):
        self.acquire_kwargs = kwargs
        timeout = kwargs.get("timeout")
        slow = SlowAcquire(self.acquire_delay)
        if timeout is not None:
            return _TimeoutSlowAcquire(slow, timeout)
        return slow

    def host(self, pool):
        return "test-host:5432"

    def register_connection(self, connection, pool):
        pass


class OneConnectionPoolManager(TestPoolManager):
    async def _pool_factory(self, dsn):
        return TestPool(str(dsn), maxsize=1)


class ReacquiringOneConnectionPoolManager(OneConnectionPoolManager):
    async def _periodic_pool_check(self, pool, dsn, sys_connection):
        await asyncio.wait_for(
            self._refresh_pool_role(pool, dsn, sys_connection),
            timeout=self._refresh_timeout,
        )
        await self._notify_about_pool_has_checked(dsn)


async def wait_until(predicate, timeout: float = 1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition was not met before timeout")
        await asyncio.sleep(0.01)


async def test_acquire_timeout_uses_shared_budget():
    pool_manager = RecordingPoolManager(pool_delay=0.05, acquire_delay=1.0)
    context = PoolAcquireContext(
        pool_manager=pool_manager,
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
    assert pool_manager.acquire_kwargs is not None
    assert 0 < pool_manager.acquire_kwargs["timeout"] < 0.1


async def test_refresh_timeout_removes_pool_from_available_set():
    pool_manager = ReacquiringOneConnectionPoolManager(
        "postgresql://test:test@master:5432/test",
        refresh_timeout=0.2,
        refresh_delay=0.2,
        acquire_timeout=0.5,
    )
    try:
        await pool_manager.ready()
        connection = await pool_manager.acquire_master()

        await wait_until(lambda: pool_manager.master_pool_count == 0)

        await pool_manager.release(connection)
        await wait_until(lambda: pool_manager.master_pool_count == 1)
    finally:
        await pool_manager.close()


async def test_close_preserves_cancellation_during_sys_connection_release():
    pool_manager = TestPoolManager(
        "postgresql://test:test@master,replica1,replica2/test",
        refresh_timeout=0.2,
        refresh_delay=0.05,
    )
    try:
        await pool_manager.ready()
        refresh_tasks = list(pool_manager._refresh_role_tasks)
        pool_manager.release_to_pool = AsyncMock(
            side_effect=asyncio.CancelledError(),
        )

        await pool_manager.close()

        assert pool_manager.closed
        assert pool_manager.release_to_pool.await_count > 0
        assert all(task.done() for task in refresh_tasks)
    finally:
        if not pool_manager.closed:
            await pool_manager.close()
