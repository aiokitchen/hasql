import random
from collections.abc import Iterable

from .base import AbstractBalancerPolicy, PoolT


class RandomWeightedBalancerPolicy(AbstractBalancerPolicy[PoolT]):
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

        weights = self._compute_weights(
            self._pool_state.get_last_response_time(pool)
            for pool in candidates
        )
        return random.choices(candidates, weights=weights)[0]

    @staticmethod
    def _compute_weights(
        times: Iterable[float | None],
    ) -> list[float]:
        values = [0 if t is None else t for t in times]
        max_time = max(values) if values else 0
        # Reflect: faster (lower time) gets higher weight.
        # +1 ensures all-zero case produces uniform weights
        # rather than all-zero weights (which random.choices rejects).
        return [max_time - v + 1 for v in values]


__all__ = ["RandomWeightedBalancerPolicy"]
