import random

from hasql.balancer_policy.base import AbstractBalancerPolicy, PoolT


class GreedyBalancerPolicy(AbstractBalancerPolicy[PoolT]):
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

        freesizes = [
            (candidate, self._pool_state.get_pool_freesize(candidate))
            for candidate in candidates
        ]

        max_freesize = max(freesize for _, freesize in freesizes)
        best = [
            candidate
            for candidate, freesize in freesizes
            if freesize == max_freesize
        ]

        return random.choice(best)


__all__ = ("GreedyBalancerPolicy",)
