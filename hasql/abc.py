from abc import ABC, abstractmethod
from typing import Generic, Optional, Sequence, TypeVar

from .acquire import AcquireContext
from .metrics import DriverMetrics
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
        timeout: Optional[float] = None,
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
    def driver_metrics(
        self,
        pools: Sequence[Optional[PoolT]],
    ) -> Sequence[DriverMetrics]: ...

    def prepare_pool_factory_kwargs(self, kwargs: dict) -> dict:
        """Hook for drivers to adjust pool factory kwargs."""
        return kwargs


__all__ = ("PoolDriver",)
