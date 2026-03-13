import asyncio
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    Generator,
    Generic,
    Optional,
    Protocol,
    TypeVar,
)

from .metrics import CalculateMetrics

if TYPE_CHECKING:
    from .pool_manager import BasePoolManager

PoolT = TypeVar("PoolT")
ConnT = TypeVar("ConnT")
ConnT_co = TypeVar("ConnT_co", covariant=True)


class AcquireContext(Protocol[ConnT_co]):
    async def __aenter__(self) -> ConnT: ...
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]: ...
    def __await__(self) -> Generator[Any, None, ConnT]: ...


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

    async def __aexit__(self, *exc) -> None:
        # TODO: consider adding a bounded timeout here. Currently if the
        #  underlying driver hangs during connection release this will block
        #  indefinitely. A timeout risks leaking the connection (not returned
        #  to pool), so this needs careful design.
        await self._context.__aexit__(*exc)

    def __await__(self) -> Generator[Any, None, ConnT]:
        return asyncio.wait_for(
            self._context.__aenter__(),
            timeout=self._timeout,
        ).__await__()


class PoolAcquireContext(AsyncContextManager[ConnT], Generic[PoolT, ConnT]):
    def __init__(
        self,
        pool_manager: "BasePoolManager[PoolT, ConnT]",
        read_only: bool,
        master_as_replica_weight: Optional[float],
        timeout: float,
        metrics: CalculateMetrics,
        fallback_master: bool = False,
        **kwargs,
    ):
        self.pool_manager = pool_manager
        self.read_only = read_only
        self.fallback_master = fallback_master
        self.master_as_replica_weight = master_as_replica_weight
        self.timeout = timeout
        self.kwargs = kwargs
        self.metrics = metrics

    def _deadline(self) -> float:
        return asyncio.get_running_loop().time() + self.timeout

    def _remaining_timeout(self, deadline: float) -> float:
        remaining_timeout = deadline - asyncio.get_running_loop().time()
        if remaining_timeout <= 0:
            raise asyncio.TimeoutError
        return remaining_timeout

    async def _get_pool(self, deadline: float) -> PoolT:
        async def get_pool() -> PoolT:
            balancer = self.pool_manager.balancer
            if balancer is None:
                raise RuntimeError("Pool manager is closed")
            with self.metrics.with_get_pool():
                pool = await balancer.get_pool(
                    read_only=self.read_only,
                    fallback_master=self.fallback_master,
                    master_as_replica_weight=self.master_as_replica_weight,
                )
            if pool is None:
                raise RuntimeError("No available pool")
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
        driver_ctx = self.pool_manager.acquire_from_pool(
            pool,
            timeout=remaining,
            **self.kwargs,
        )
        return pool, driver_ctx

    async def _acquire_connection(self) -> ConnT:
        pool, driver_ctx = await self._resolve_pool_and_acquire_context()

        with self.metrics.with_acquire(self.pool_manager.host(pool)):
            conn: ConnT = await driver_ctx

        self.metrics.add_connection(self.pool_manager.host(pool))
        self.pool_manager.register_connection(conn, pool)
        return conn

    async def __aenter__(self) -> ConnT:
        pool, driver_ctx = await self._resolve_pool_and_acquire_context()

        with self.metrics.with_acquire(self.pool_manager.host(pool)):
            conn: ConnT = await driver_ctx.__aenter__()

        self.metrics.add_connection(self.pool_manager.host(pool))
        self._pool = pool
        self._context = driver_ctx
        return conn

    async def __aexit__(self, *exc):
        self.metrics.remove_connection(
            self.pool_manager.host(self._pool),
        )
        await self._context.__aexit__(*exc)

    def __await__(self):
        return self._acquire_connection().__await__()


__all__ = (
    "AcquireContext",
    "TimeoutAcquireContext",
    "PoolAcquireContext",
)
