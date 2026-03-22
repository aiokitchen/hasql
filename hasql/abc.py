import warnings
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Generic, TypeVar

from .acquire import AcquireContext
from .metrics import DriverMetrics, PoolStats
from .utils import Dsn

PoolT = TypeVar("PoolT")
ConnT = TypeVar("ConnT")


class PoolDriver(ABC, Generic[PoolT, ConnT]):
    """Database driver interface for pool operations."""

    @abstractmethod
    def get_pool_freesize(self, pool: PoolT) -> int: ...

    @abstractmethod
    def acquire_from_pool(
        self,
        pool: PoolT,
        *,
        timeout: float | None = None,
        **kwargs,
    ) -> AcquireContext[ConnT]: ...

    @abstractmethod
    async def release_to_pool(
        self,
        connection: ConnT,
        pool: PoolT,
        **kwargs,
    ) -> None: ...

    @abstractmethod
    async def is_master(self, connection: ConnT) -> bool: ...

    @abstractmethod
    async def pool_factory(self, dsn: Dsn, **kwargs) -> PoolT: ...

    @abstractmethod
    async def close_pool(self, pool: PoolT) -> None: ...

    @abstractmethod
    async def terminate_pool(self, pool: PoolT) -> None: ...

    @abstractmethod
    def is_connection_closed(self, connection: ConnT) -> bool: ...

    @abstractmethod
    def host(self, pool: PoolT) -> str: ...

    @abstractmethod
    def pool_stats(self, pool: PoolT) -> PoolStats: ...

    def driver_metrics(
        self,
        pools: Sequence[PoolT | None],
    ) -> Sequence[DriverMetrics]:
        warnings.warn(
            "driver_metrics() is deprecated, implement pool_stats() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return [
            DriverMetrics(
                min=s.min, max=s.max, idle=s.idle, used=s.used,
                host=self.host(p),
            )
            for p in pools if p
            for s in [self.pool_stats(p)]
        ]

    def prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        """Hook for drivers to adjust pool factory kwargs."""
        return kwargs


__all__ = ("PoolDriver",)
