import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from hasql.acquire import TimeoutAcquireContext, PoolAcquireContext
from hasql.metrics import CalculateMetrics


class FakeAcquireContext:
    def __init__(self, conn=None):
        self.conn = conn or object()
        self.entered = False
        self.exited = False
        self.exit_args = None

    async def __aenter__(self):
        self.entered = True
        return self.conn

    async def __aexit__(self, *exc):
        self.exited = True
        self.exit_args = exc

    def __await__(self):
        return self.__aenter__().__await__()


def _make_mock_pool_manager(pool, inner_ctx):
    """Create a MagicMock that mimics the new private API."""
    pm = MagicMock()

    pool_state = MagicMock()
    pool_state.host.return_value = "test-host:5432"
    pool_state.acquire_from_pool.return_value = inner_ctx
    pm._pool_state = pool_state

    pm._balancer = MagicMock()
    pm._balancer.get_pool = AsyncMock(return_value=pool)

    pm._register_connection = MagicMock()
    pm._unregister_connection = MagicMock()

    return pm


async def test_timeout_acquire_context_aenter():
    conn = object()
    ctx = TimeoutAcquireContext(FakeAcquireContext(conn), timeout=1.0)
    result = await ctx.__aenter__()
    assert result is conn


async def test_timeout_acquire_context_aexit_delegates():
    inner = FakeAcquireContext()
    ctx = TimeoutAcquireContext(inner, timeout=1.0)
    await ctx.__aenter__()
    await ctx.__aexit__(None, None, None)
    assert inner.exited


async def test_timeout_acquire_context_await():
    conn = object()
    ctx = TimeoutAcquireContext(FakeAcquireContext(conn), timeout=1.0)
    result = await ctx
    assert result is conn


async def test_timeout_acquire_context_timeout_fires():
    class SlowContext:
        async def __aenter__(self):
            await asyncio.sleep(10)
            return object()

        async def __aexit__(self, *exc):
            pass

        def __await__(self):
            return self.__aenter__().__await__()

    ctx = TimeoutAcquireContext(SlowContext(), timeout=0.01)
    with pytest.raises(asyncio.TimeoutError):
        await ctx


async def test_pool_acquire_context_aexit_removes_connection():
    metrics = CalculateMetrics()
    pool = object()
    inner_ctx = FakeAcquireContext()

    pm = _make_mock_pool_manager(pool, inner_ctx)

    ctx = PoolAcquireContext(
        pool_manager=pm,
        read_only=False,
        fallback_master=False,
        master_as_replica_weight=None,
        timeout=1.0,
        metrics=metrics,
    )

    await ctx.__aenter__()
    assert metrics._add_connections.get("test-host:5432") == 1

    await ctx.__aexit__(None, None, None)
    assert metrics._remove_connections.get("test-host:5432") == 1
    assert inner_ctx.exited


async def test_pool_acquire_context_await_registers_connection():
    metrics = CalculateMetrics()
    pool = object()
    conn = object()
    inner_ctx = FakeAcquireContext(conn)

    pm = _make_mock_pool_manager(pool, inner_ctx)

    ctx = PoolAcquireContext(
        pool_manager=pm,
        read_only=False,
        fallback_master=False,
        master_as_replica_weight=None,
        timeout=1.0,
        metrics=metrics,
    )

    result = await ctx
    assert result is conn
    pm._register_connection.assert_called_once_with(conn, pool)
    assert metrics._add_connections.get("test-host:5432") == 1


async def test_pool_acquire_context_remaining_timeout_raises():
    metrics = CalculateMetrics()
    pm = MagicMock()

    ctx = PoolAcquireContext(
        pool_manager=pm,
        read_only=False,
        fallback_master=False,
        master_as_replica_weight=None,
        timeout=1.0,
        metrics=metrics,
    )

    past_deadline = asyncio.get_running_loop().time() - 1.0
    with pytest.raises(asyncio.TimeoutError):
        ctx._remaining_timeout(past_deadline)


async def test_pool_acquire_context_deadline():
    metrics = CalculateMetrics()
    pm = MagicMock()

    ctx = PoolAcquireContext(
        pool_manager=pm,
        read_only=False,
        fallback_master=False,
        master_as_replica_weight=None,
        timeout=5.0,
        metrics=metrics,
    )

    now = asyncio.get_running_loop().time()
    deadline = ctx._deadline()
    assert deadline > now
    assert deadline <= now + 5.1


async def test_pool_acquire_context_aenter_cleans_up_on_register_failure():
    """If _register_connection raises after driver_ctx.__aenter__ succeeds,
    the driver context must still be cleaned up via __aexit__."""
    metrics = CalculateMetrics()
    pool = object()
    inner_ctx = FakeAcquireContext()

    pm = _make_mock_pool_manager(pool, inner_ctx)
    pm._register_connection.side_effect = RuntimeError("register failed")

    ctx = PoolAcquireContext(
        pool_manager=pm,
        read_only=False,
        fallback_master=False,
        master_as_replica_weight=None,
        timeout=1.0,
        metrics=metrics,
    )

    with pytest.raises(RuntimeError, match="register failed"):
        await ctx.__aenter__()

    assert inner_ctx.entered
    assert inner_ctx.exited, "driver context __aexit__ must be called on failure"


async def test_pool_acquire_context_await_cleans_up_on_register_failure():
    """If _register_connection raises after await driver_ctx succeeds,
    the connection must be released back to the pool."""
    metrics = CalculateMetrics()
    pool = object()
    inner_ctx = FakeAcquireContext()

    pm = _make_mock_pool_manager(pool, inner_ctx)
    pm._register_connection.side_effect = RuntimeError("register failed")
    pm._pool_state.release_to_pool = AsyncMock()

    ctx = PoolAcquireContext(
        pool_manager=pm,
        read_only=False,
        fallback_master=False,
        master_as_replica_weight=None,
        timeout=1.0,
        metrics=metrics,
    )

    with pytest.raises(RuntimeError, match="register failed"):
        await ctx

    pm._pool_state.release_to_pool.assert_awaited_once_with(
        inner_ctx.conn, pool,
    )


async def test_pool_acquire_context_kwargs_passed_through():
    metrics = CalculateMetrics()
    pool = object()
    inner_ctx = FakeAcquireContext()

    pm = _make_mock_pool_manager(pool, inner_ctx)

    ctx = PoolAcquireContext(
        pool_manager=pm,
        read_only=False,
        fallback_master=False,
        master_as_replica_weight=None,
        timeout=1.0,
        metrics=metrics,
        custom_kwarg="value",
    )

    await ctx
    call_kwargs = pm._pool_state.acquire_from_pool.call_args
    assert call_kwargs.kwargs.get("custom_kwarg") == "value"
