import math
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

        response_times = self._get_response_times(candidates)
        weights = self._inverse_latency_weights(response_times)
        return random.choices(candidates, weights=weights, k=1)[0]

    def _get_response_times(
        self,
        pools: list[PoolT],
    ) -> Iterable[float | None]:
        for pool in pools:
            yield self._pool_state.get_last_response_time(pool)

    @staticmethod
    def _inverse_latency_weights(
        times: Iterable[float | None],
    ) -> list[float]:
        list_times = list(times)
        valid_times: list[float] = []
        for value in list_times:
            if value is None or value <= 0 or not math.isfinite(value):
                return [1.0] * len(list_times)
            valid_times.append(value)

        if not valid_times:
            return []

        minimum_latency = min(valid_times)
        return [minimum_latency / value for value in valid_times]


__all__ = ["RandomWeightedBalancerPolicy"]
