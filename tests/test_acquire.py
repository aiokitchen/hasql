import asyncio

import pytest

from hasql.acquire import TimeoutAcquireContext


class FakeAcquireContext:
    def __init__(self, conn=None):
        self.conn = conn or object()
        self.entered = False
        self.exited = False
        self.exit_args = None
        self.exit_result = None

    async def __aenter__(self):
        self.entered = True
        return self.conn

    async def __aexit__(self, *exc):
        self.exited = True
        self.exit_args = exc
        return self.exit_result

    def __await__(self):
        return self.__aenter__().__await__()


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
    class SlowAcquireContext(FakeAcquireContext):
        async def __aenter__(self):
            await asyncio.sleep(1)
            return self.conn

    ctx = TimeoutAcquireContext(SlowAcquireContext(), timeout=0.01)

    with pytest.raises(asyncio.TimeoutError):
        await ctx


async def test_timeout_acquire_context_aexit_propagates_return():
    inner = FakeAcquireContext()
    inner.exit_result = True
    ctx = TimeoutAcquireContext(inner, timeout=1.0)

    result = await ctx.__aexit__(None, None, None)

    assert result is True
