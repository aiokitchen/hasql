import random
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from ..pool_state import PoolStateProvider

PoolT = TypeVar("PoolT")


class AbstractBalancerPolicy(ABC, Generic[PoolT]):
    def __init__(self, pool_state: PoolStateProvider[PoolT]):
        self._pool_state = pool_state

    async def get_pool(
        self,
        read_only: bool,
        fallback_master: bool = False,
        master_as_replica_weight: float | None = None,
    ) -> PoolT | None:
        if not read_only and master_as_replica_weight is not None:
            raise ValueError(
                "Field master_as_replica_weight is used only when "
                "read_only is True",
            )

        choose_master_as_replica = False
        if master_as_replica_weight is not None:
            choose_master_as_replica = (
                random.random() < master_as_replica_weight
            )

        return await self._get_pool(
            read_only=read_only,
            fallback_master=fallback_master or choose_master_as_replica,
            choose_master_as_replica=choose_master_as_replica,
        )

    async def _get_candidates(
        self,
        read_only: bool,
        fallback_master: bool = False,
        choose_master_as_replica: bool = False,
    ) -> list[PoolT]:
        candidates: list[PoolT] = []

        if read_only:
            candidates.extend(
                await self._pool_state.get_replica_pools(
                    fallback_master=fallback_master,
                ),
            )

        if not read_only or (
            choose_master_as_replica
            and self._pool_state.master_pool_count > 0
            and self._pool_state.replica_pool_count > 0
        ):
            candidates.extend(await self._pool_state.get_master_pools())

        return candidates

    @abstractmethod
    async def _get_pool(
        self,
        read_only: bool,
        fallback_master: bool = False,
        choose_master_as_replica: bool = False,
    ) -> PoolT | None:
        pass


# Backward-compatible alias
BaseBalancerPolicy = AbstractBalancerPolicy

__all__ = ["AbstractBalancerPolicy", "BaseBalancerPolicy"]
