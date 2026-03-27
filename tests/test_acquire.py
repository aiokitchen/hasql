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


def _make_mocks(pool, inner_ctx):
    """Create mocks for PoolAcquireContext dependencies."""
    pool_state = MagicMock()
    pool_state.host.return_value = "test-host:5432"
    pool_state.acquire_from_pool.return_value = inner_ctx

    balancer = MagicMock()
    balancer.get_pool = AsyncMock(return_value=pool)

    register = MagicMock()
    unregister = MagicMock()

    return pool_state, balancer, register, unregister


def _make_ctx(pool_state, balancer, register, unregister, metrics, **kwargs):
    """Create a PoolAcquireContext with the given mocks."""
    defaults = dict(
        read_only=False,
        fallback_master=False,
        master_as_replica_weight=None,
        timeout=1.0,
    )
    defaults.update(kwargs)
    return PoolAcquireContext(
        pool_state=pool_state,
        balancer=balancer,
        register_connection=register,
        unregister_connection=unregister,
        metrics=metrics,
        **defaults,
    )


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
    pool_state, balancer, register, unregister = _make_mocks(pool, inner_ctx)

    ctx = _make_ctx(pool_state, balancer, register, unregister, metrics)

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
    pool_state, balancer, register, unregister = _make_mocks(pool, inner_ctx)

    ctx = _make_ctx(pool_state, balancer, register, unregister, metrics)

    result = await ctx
    assert result is conn
    register.assert_called_once_with(conn, pool)
    assert metrics._add_connections.get("test-host:5432") == 1


async def test_pool_acquire_context_remaining_timeout_raises():
    metrics = CalculateMetrics()
    pool_state, balancer, register, unregister = (
        MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )

    ctx = _make_ctx(pool_state, balancer, register, unregister, metrics)

    past_deadline = asyncio.get_running_loop().time() - 1.0
    with pytest.raises(asyncio.TimeoutError):
        ctx._remaining_timeout(past_deadline)


async def test_pool_acquire_context_deadline():
    metrics = CalculateMetrics()
    pool_state, balancer, register, unregister = (
        MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )

    ctx = _make_ctx(
        pool_state, balancer, register, unregister, metrics, timeout=5.0,
    )

    now = asyncio.get_running_loop().time()
    deadline = ctx._deadline()
    assert deadline > now
    assert deadline <= now + 5.1


async def test_pool_acquire_context_aenter_cleans_up_on_register_failure():
    """If register_connection raises after driver_ctx.__aenter__ succeeds,
    the driver context must still be cleaned up via __aexit__."""
    metrics = CalculateMetrics()
    pool = object()
    inner_ctx = FakeAcquireContext()
    pool_state, balancer, register, unregister = _make_mocks(pool, inner_ctx)
    register.side_effect = RuntimeError("register failed")

    ctx = _make_ctx(pool_state, balancer, register, unregister, metrics)

    with pytest.raises(RuntimeError, match="register failed"):
        await ctx.__aenter__()

    assert inner_ctx.entered
    assert inner_ctx.exited, (
        "driver context __aexit__ must be called on failure"
    )


async def test_pool_acquire_context_await_cleans_up_on_register_failure():
    """If register_connection raises after await driver_ctx succeeds,
    the connection must be released back to the pool."""
    metrics = CalculateMetrics()
    pool = object()
    inner_ctx = FakeAcquireContext()
    pool_state, balancer, register, unregister = _make_mocks(pool, inner_ctx)
    register.side_effect = RuntimeError("register failed")
    pool_state.release_to_pool = AsyncMock()

    ctx = _make_ctx(pool_state, balancer, register, unregister, metrics)

    with pytest.raises(RuntimeError, match="register failed"):
        await ctx

    pool_state.release_to_pool.assert_awaited_once_with(
        inner_ctx.conn, pool,
    )


async def test_pool_acquire_context_kwargs_passed_through():
    metrics = CalculateMetrics()
    pool = object()
    inner_ctx = FakeAcquireContext()
    pool_state, balancer, register, unregister = _make_mocks(pool, inner_ctx)

    ctx = _make_ctx(
        pool_state, balancer, register, unregister, metrics,
        custom_kwarg="value",
    )

    await ctx
    call_kwargs = pool_state.acquire_from_pool.call_args
    assert call_kwargs.kwargs.get("custom_kwarg") == "value"
