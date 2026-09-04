import asyncio
from collections.abc import Callable, Generator
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Protocol,
    TypeVar,
)

from .exceptions import NoAvailablePoolError
from .metrics import CalculateMetrics

if TYPE_CHECKING:
    from .balancer_policy.base import AbstractBalancerPolicy
    from .pool_state import PoolState

PoolT = TypeVar("PoolT")
ConnT = TypeVar("ConnT")
ConnT_co = TypeVar("ConnT_co", covariant=True)


class AcquireContext(Protocol[ConnT_co]):
    async def __aenter__(self) -> ConnT_co: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None: ...
    def __await__(self) -> Generator[Any, None, ConnT_co]: ...


class TimeoutAcquireContext(Generic[ConnT]):
    __slots__ = ("_context", "_timeout")

    def __init__(self, context: AcquireContext[ConnT], timeout: float):
        self._context = context
        self._timeout = timeout

    async def __aenter__(self) -> ConnT:
        return await asyncio.wait_for(
            self._context.__aenter__(),
            timeout=self._timeout,
        )

    async def __aexit__(self, *exc):
        # TODO: consider adding a bounded timeout here. Currently if the
        #  underlying driver hangs during connection release this will block
        #  indefinitely. A timeout risks leaking the connection (not returned
        #  to pool), so this needs careful design.
        return await self._context.__aexit__(*exc)

    def __await__(self) -> Generator[Any, None, ConnT]:
        return asyncio.wait_for(
            self._context.__aenter__(),
            timeout=self._timeout,
        ).__await__()


class PoolAcquireContext(
    AbstractAsyncContextManager[ConnT],
    Generic[PoolT, ConnT],
):
    def __init__(
        self,
        pool_state: "PoolState[PoolT, ConnT]",
        balancer: "AbstractBalancerPolicy[PoolT]",
        register_connection: Callable[[ConnT, PoolT], None],
        unregister_connection: Callable[[ConnT], None],
        read_only: bool,
        master_as_replica_weight: float | None,
        timeout: float,
        metrics: CalculateMetrics,
        fallback_master: bool = False,
        **kwargs,
    ):
        self._pool_state = pool_state
        self._balancer = balancer
        self._register_connection = register_connection
        self._unregister_connection = unregister_connection
        self._read_only = read_only
        self._fallback_master = fallback_master
        self._master_as_replica_weight = master_as_replica_weight
        self._timeout = timeout
        self._kwargs = kwargs
        self._metrics = metrics
        self._pool: PoolT | None = None
        self._conn: ConnT | None = None
        self._context: AcquireContext[ConnT] | None = None

    def _deadline(self) -> float:
        return asyncio.get_running_loop().time() + self._timeout

    def _remaining_timeout(self, deadline: float) -> float:
        remaining_timeout = deadline - asyncio.get_running_loop().time()
        if remaining_timeout <= 0:
            raise asyncio.TimeoutError
        return remaining_timeout

    async def _get_pool(self, deadline: float) -> PoolT:
        async def get_pool() -> PoolT:
            with self._metrics.with_get_pool():
                pool = await self._balancer.get_pool(
                    read_only=self._read_only,
                    fallback_master=self._fallback_master,
                    master_as_replica_weight=self._master_as_replica_weight,
                )
            if pool is None:
                raise NoAvailablePoolError("No available pool")
            return pool

        return await asyncio.wait_for(
            get_pool(),
            timeout=self._remaining_timeout(deadline),
        )

    async def _resolve_pool_and_acquire_context(
        self,
    ) -> tuple[PoolT, AcquireContext[ConnT]]:
        deadline = self._deadline()
        pool = await self._get_pool(deadline)
        remaining = self._remaining_timeout(deadline)
        driver_ctx = self._pool_state.acquire_from_pool(
            pool,
            timeout=remaining,
            **self._kwargs,
        )
        return pool, driver_ctx

    async def _acquire_connection(self) -> ConnT:
        pool, driver_ctx = await self._resolve_pool_and_acquire_context()

        host = self._pool_state.host(pool)
        with self._metrics.with_acquire(host):
            conn: ConnT = await driver_ctx

        try:
            self._metrics.add_connection(host)
            self._register_connection(conn, pool)
        except BaseException:
            await self._pool_state.release_to_pool(conn, pool)
            raise
        return conn

    async def __aenter__(self) -> ConnT:
        pool, driver_ctx = await self._resolve_pool_and_acquire_context()

        host = self._pool_state.host(pool)
        with self._metrics.with_acquire(host):
            conn: ConnT = await driver_ctx.__aenter__()

        try:
            self._metrics.add_connection(host)
            self._register_connection(conn, pool)
        except BaseException:
            await driver_ctx.__aexit__(None, None, None)
            raise

        self._pool = pool
        self._conn = conn
        self._context = driver_ctx
        return conn

    async def __aexit__(self, *exc):
        if self._conn is None or self._pool is None or self._context is None:
            return
        self._unregister_connection(self._conn)
        self._metrics.remove_connection(
            self._pool_state.host(self._pool),
        )
        await self._context.__aexit__(*exc)

    def __await__(self):
        return self._acquire_connection().__await__()


__all__ = (
    "AcquireContext",
    "TimeoutAcquireContext",
    "PoolAcquireContext",
)
