from collections import defaultdict
from typing import NamedTuple

from hasql.balancer_policy.base import AbstractBalancerPolicy, PoolT
from hasql.pool_state import PoolStateProvider


class PoolOptions(NamedTuple):
    read_only: bool
    choose_master_as_replica: bool


class RoundRobinBalancerPolicy(AbstractBalancerPolicy[PoolT]):
    def __init__(self, pool_state: PoolStateProvider[PoolT]):
        super().__init__(pool_state)
        self._indexes: defaultdict[PoolOptions, int] = defaultdict(lambda: 0)

    async def _get_pool(
        self,
        read_only: bool,
        fallback_master: bool = False,
        choose_master_as_replica: bool = False,
    ) -> PoolT | None:
        candidates = await self._get_candidates(
            read_only=read_only,
            fallback_master=fallback_master,
            choose_master_as_replica=choose_master_as_replica,
        )

        if not candidates:
            return None

        pool_options = PoolOptions(read_only, choose_master_as_replica)
        start_index = self._indexes[pool_options]
        index = start_index % len(candidates)
        self._indexes[pool_options] = (start_index + 1) % len(candidates)
        return candidates[index]


__all__ = ("RoundRobinBalancerPolicy",)
