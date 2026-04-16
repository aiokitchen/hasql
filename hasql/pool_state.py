# Minimal stub for PR2 — full PoolState implementation is in PR3.
from typing import Protocol, TypeVar, runtime_checkable

PoolT = TypeVar("PoolT")


@runtime_checkable
class PoolStateProvider(Protocol[PoolT]):
    """Protocol for read access to pool sets — used by balancer policies."""

    @property
    def master_pool_count(self) -> int: ...

    @property
    def replica_pool_count(self) -> int: ...

    async def get_master_pools(self) -> list[PoolT]: ...

    async def get_replica_pools(
        self, fallback_master: bool = False,
    ) -> list[PoolT]: ...

    def get_pool_freesize(self, pool: PoolT) -> int: ...

    def get_last_response_time(self, pool: PoolT) -> float | None: ...


__all__ = ("PoolStateProvider",)
