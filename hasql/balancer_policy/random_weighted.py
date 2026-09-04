import random
from collections.abc import Iterable

from .base import AbstractBalancerPolicy, PoolT


MACHINE_EPSILON: float = 1e-16


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

        chosen_index = self._weighted_choice(
            self._normalize_times(
                self._reflect_times(
                    self._get_response_times(candidates),
                ),
            ),
        )
        return candidates[chosen_index]

    def _get_response_times(
        self,
        pools: list[PoolT],
    ) -> Iterable[float | None]:
        for pool in pools:
            yield self._pool_state.get_last_response_time(pool)

    @staticmethod
    def _reflect_times(
        times: Iterable[float | None],
    ) -> Iterable[float]:
        list_times = [value or 0 for value in times]
        sum_time = sum(list_times)
        yield from map(
            lambda value: sum_time - value + MACHINE_EPSILON,
            list_times,
        )

    @staticmethod
    def _normalize_times(times: Iterable[float]) -> Iterable[float]:
        list_times = list(times)
        sum_time = sum(list_times)
        yield from map(lambda value: sum_time / value, list_times)

    @staticmethod
    def _weighted_choice(probability_distribution: Iterable[float]) -> int:
        rand = random.random()
        prefix_sum = 0.0
        length = 0
        for index, probability in enumerate(probability_distribution):
            length += 1
            prefix_sum += probability
            if rand <= prefix_sum:
                return index
        return length - 1


__all__ = ["RandomWeightedBalancerPolicy"]
