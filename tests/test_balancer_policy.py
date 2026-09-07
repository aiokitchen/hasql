import asyncio
import math
import random

import pytest
from async_timeout import timeout

from hasql.balancer_policy import (
    GreedyBalancerPolicy,
    RandomWeightedBalancerPolicy,
    RoundRobinBalancerPolicy,
)
from hasql.pool_state import PoolState
from hasql.utils import Dsn
from tests.mocks import TestPoolManager
from tests.mocks.pool_manager import TestDriver, TestPool

balancer_policies = pytest.mark.parametrize(
    "balancer_policy",
    [
        GreedyBalancerPolicy,
        RandomWeightedBalancerPolicy,
        RoundRobinBalancerPolicy,
    ],
)


class StablePoolStateProvider:
    def __init__(
        self,
        pools: list[object],
        response_times: list[float | None],
    ) -> None:
        self._pools = pools
        self._response_times = dict(zip(pools, response_times, strict=True))

    @property
    def master_pool_count(self) -> int:
        return 0

    @property
    def replica_pool_count(self) -> int:
        return len(self._pools)

    @property
    def stale_pool_count(self) -> int:
        return 0

    async def get_master_pools(self) -> list[object]:
        return []

    async def get_replica_pools(
        self,
        fallback_master: bool = False,
    ) -> list[object]:
        return list(self._pools)

    def get_stale_pools(self) -> list[object]:
        return []

    def get_pool_freesize(self, pool: object) -> int:
        return 1

    def get_last_response_time(self, pool: object) -> float | None:
        return self._response_times[pool]


@pytest.fixture
def make_dsn():
    def make(replicas_count: int):
        dsn = "postgresql://test:test@master:5432"
        replica_hosts = [f"replica{i}" for i in range(1, replicas_count + 1)]
        if replica_hosts:
            dsn += "," + ",".join(replica_hosts)
        return dsn + "/test"

    return make


@pytest.fixture
async def make_pool_manager(make_dsn):
    pool_managers = []

    async def make(balancer_policy, replicas_count: int = 2):
        pool_manager = TestPoolManager(
            dsn=make_dsn(replicas_count),
            balancer_policy=balancer_policy,
            refresh_timeout=0.2,
            refresh_delay=0.1,
            acquire_timeout=0.1,
        )
        pool_managers.append(pool_manager)
        return pool_manager

    try:
        yield make
    finally:
        await asyncio.gather(
            *(pool_manager.close() for pool_manager in pool_managers),
            return_exceptions=True,
        )


@balancer_policies
async def test_acquire_master(make_pool_manager, balancer_policy):
    pool_manager = await make_pool_manager(balancer_policy)
    async with timeout(1):
        async with pool_manager.acquire_master() as conn:
            assert await conn.is_master()


@balancer_policies
async def test_acquire_replica(make_pool_manager, balancer_policy):
    pool_manager = await make_pool_manager(balancer_policy)
    async with timeout(1):
        async with pool_manager.acquire_replica() as conn:
            assert not await conn.is_master()


@balancer_policies
async def test_acquire_replica_with_fallback_master(
    make_pool_manager,
    balancer_policy,
):
    pool_manager = await make_pool_manager(balancer_policy, replicas_count=0)
    async with timeout(1):
        async with pool_manager.acquire_replica(fallback_master=True) as conn:
            assert await conn.is_master()


@balancer_policies
async def test_acquire_master_as_replica(make_pool_manager, balancer_policy):
    pool_manager = await make_pool_manager(balancer_policy, replicas_count=0)
    async with timeout(1):
        async with pool_manager.acquire_replica(
            master_as_replica_weight=1.0,
        ) as conn:
            assert await conn.is_master()


@balancer_policies
async def test_dont_acquire_master_as_replica(
    make_pool_manager,
    balancer_policy,
):
    pool_manager = await make_pool_manager(balancer_policy, replicas_count=0)
    with pytest.raises(asyncio.TimeoutError):
        async with pool_manager.acquire_replica(master_as_replica_weight=0.0):
            pass


@balancer_policies
async def test_master_as_replica_weight_zero_always_false(
    make_pool_manager,
    balancer_policy,
):
    """weight=0 should never choose master as replica, even when rand=0."""
    pool_manager = await make_pool_manager(balancer_policy, replicas_count=2)
    async with timeout(1):
        await pool_manager._pool_state.ready()

    # With weight=0, should never get master when requesting replica
    for _ in range(20):
        pool = await pool_manager._balancer.get_pool(
            read_only=True,
            master_as_replica_weight=0.0,
        )
        assert pool is None or pool_manager._pool_state.pool_is_replica(pool)


@balancer_policies
async def test_get_pool_write_with_master_as_replica_weight_raises(
    make_pool_manager,
    balancer_policy,
):
    pool_manager = await make_pool_manager(balancer_policy)
    async with timeout(1):
        await pool_manager._pool_state.ready()
    with pytest.raises(ValueError, match="master_as_replica_weight"):
        await pool_manager._balancer.get_pool(
            read_only=False,
            master_as_replica_weight=0.5,
        )


def test_random_weighted_inverse_latency_weights_use_inverse_ratios():
    weights = RandomWeightedBalancerPolicy._inverse_latency_weights(
        [1.0, 2.0, 4.0],
    )

    assert weights == pytest.approx([1.0, 0.5, 0.25])


def test_random_weighted_inverse_latency_weights_are_equal_for_equal_times():
    weights = RandomWeightedBalancerPolicy._inverse_latency_weights(
        [0.25, 0.25, 0.25],
    )

    assert weights == [1.0, 1.0, 1.0]


@pytest.mark.parametrize(
    "invalid_time",
    [None, 0.0, -1.0, float("nan"), float("inf"), float("-inf")],
)
def test_random_weighted_invalid_latency_makes_all_weights_equal(
    invalid_time,
):
    weights = RandomWeightedBalancerPolicy._inverse_latency_weights(
        [0.5, invalid_time, 2.0],
    )

    assert weights == [1.0, 1.0, 1.0]


def test_random_weighted_inverse_latency_weights_are_not_capped():
    weights = RandomWeightedBalancerPolicy._inverse_latency_weights(
        [0.001, 1000.0],
    )

    assert (weights, weights[0] / weights[1]) == (
        pytest.approx([1.0, 1e-6]),
        pytest.approx(1e6),
    )


async def test_random_weighted_smallest_positive_latency_uses_finite_weights(
    monkeypatch,
):
    smallest_positive = math.nextafter(0.0, 1.0)
    pools = [object(), object()]
    choices_calls = []
    original_choices = random.choices

    def choose(candidates, weights, k):
        candidate_list = list(candidates)
        weight_list = list(weights)
        choices_calls.append((candidate_list, weight_list, k))
        return original_choices(candidate_list, weights=weight_list, k=k)

    monkeypatch.setattr("random.choices", choose)
    policy = RandomWeightedBalancerPolicy(
        StablePoolStateProvider(pools, [smallest_positive, 1.0]),
    )

    chosen_pool = await policy._get_pool(read_only=True)

    received_weights = choices_calls[0][1]
    assert (
        chosen_pool in pools,
        choices_calls[0][0],
        choices_calls[0][2],
        all(math.isfinite(weight) for weight in received_weights),
        all(weight > 0.0 for weight in received_weights),
    ) == (True, pools, 1, True, True)


async def test_random_weighted_get_pool_passes_candidates_and_weights(
    monkeypatch,
):
    slow_pool = object()
    fast_pool = object()
    pools = [slow_pool, fast_pool]
    choices_calls = []

    def choose(candidates, weights, k):
        candidate_list = list(candidates)
        choices_calls.append((candidate_list, list(weights), k))
        return [candidate_list[1]]

    monkeypatch.setattr("random.choices", choose)
    pool_state = StablePoolStateProvider(pools, [2.0, 0.5])
    policy = RandomWeightedBalancerPolicy(pool_state)

    chosen_pool = await policy._get_pool(read_only=True)

    assert (chosen_pool, choices_calls) == (
        fast_pool,
        [([slow_pool, fast_pool], [0.25, 1.0], 1)],
    )


async def test_random_weighted_get_pool_returns_none_without_candidates():
    policy = RandomWeightedBalancerPolicy(StablePoolStateProvider([], []))

    chosen_pool = await policy._get_pool(read_only=True)

    assert chosen_pool is None


async def test_random_weighted_get_pool_returns_the_only_candidate():
    only_pool = object()
    pool_state = StablePoolStateProvider([only_pool], [0.5])
    policy = RandomWeightedBalancerPolicy(pool_state)

    chosen_pool = await policy._get_pool(read_only=True)

    assert chosen_pool is only_pool


async def test_read_only_waiter_wakes_when_pool_becomes_stale():
    dsn = Dsn.parse("postgresql://test:test@replica:5432/test")
    pool = TestPool(str(dsn))
    pool_state = PoolState([dsn], TestDriver(), 10)
    pool_state.set_pool(0, pool)
    balancer = RoundRobinBalancerPolicy(pool_state)
    selection = asyncio.create_task(
        balancer.get_pool(read_only=True, fallback_master=True),
    )

    try:
        await asyncio.wait_for(asyncio.sleep(0), timeout=0.1)
        pool_state.mark_pool_stale(pool)
        selected = await asyncio.wait_for(
            asyncio.shield(selection), timeout=0.1,
        )
    finally:
        if not selection.done():
            selection.cancel()
        await asyncio.gather(selection, return_exceptions=True)

    assert selected is pool


async def test_round_robin_master_as_replica(make_pool_manager):
    pool_manager = await make_pool_manager(
        RoundRobinBalancerPolicy,
        replicas_count=0,
    )
    async with timeout(1):
        await pool_manager._pool_state.ready()

    async with pool_manager.acquire_replica(
        master_as_replica_weight=1.0,
    ) as conn:
        assert await conn.is_master()


async def test_round_robin_waits_for_master_when_not_ready(
    make_pool_manager,
):
    pool_manager = await make_pool_manager(
        RoundRobinBalancerPolicy,
        replicas_count=0,
    )
    async with timeout(2):
        await pool_manager._pool_state.ready()

    # Shut down the master so master_pool_count drops to 0
    ps = pool_manager._pool_state
    master_pool: TestPool = (await ps.get_master_pools())[0]
    master_pool.shutdown()

    # Wait for the health monitor to detect the shutdown
    await ps.wait_next_pool_check()
    assert ps.master_pool_count == 0

    # Bring it back after a short delay so the wait resolves
    async def bring_master_back():
        await asyncio.sleep(0.15)
        master_pool.startup()
        master_pool.set_master(True)

    asyncio.ensure_future(bring_master_back())

    # Use explicit timeout to override the short default acquire_timeout
    async with timeout(2):
        async with pool_manager.acquire_master(timeout=2) as conn:
            assert await conn.is_master()


async def test_round_robin_waits_for_replica_when_not_ready(
    make_pool_manager,
):
    pool_manager = await make_pool_manager(
        RoundRobinBalancerPolicy,
        replicas_count=2,
    )
    async with timeout(2):
        await pool_manager._pool_state.ready()

    # Shut down all replicas so replica_pool_count drops to 0
    ps = pool_manager._pool_state
    replica_pools = [
        pool
        for pool in ps.pools
        if pool is not None and ps.pool_is_replica(pool)
    ]
    for rp in replica_pools:
        rp.shutdown()

    # Wait for health monitor to detect shutdowns
    await pool_manager._pool_state.wait_next_pool_check()
    assert pool_manager._pool_state.replica_pool_count == 0

    # Bring replicas back after a short delay
    async def bring_replicas_back():
        await asyncio.sleep(0.15)
        for rp in replica_pools:
            rp.startup()

    asyncio.ensure_future(bring_replicas_back())

    # Use explicit timeout to override the short default acquire_timeout
    async with timeout(2):
        async with pool_manager.acquire_replica(timeout=2) as conn:
            assert not await conn.is_master()


async def test_round_robin_fallback_master_waits_when_master_not_ready(
    make_pool_manager,
):
    pool_manager = await make_pool_manager(
        RoundRobinBalancerPolicy,
        replicas_count=0,
    )
    async with timeout(2):
        await pool_manager._pool_state.ready()

    # Shut down the master so both master and replica counts are 0
    ps = pool_manager._pool_state
    master_pool: TestPool = (await ps.get_master_pools())[0]
    master_pool.shutdown()

    await pool_manager._pool_state.wait_next_pool_check()
    assert pool_manager._pool_state.master_pool_count == 0
    assert pool_manager._pool_state.replica_pool_count == 0

    # Bring master back after a short delay
    async def bring_master_back():
        await asyncio.sleep(0.15)
        master_pool.startup()
        master_pool.set_master(True)

    asyncio.ensure_future(bring_master_back())

    # acquire_replica with fallback_master=True should wait for master
    # Use explicit timeout to override the short default acquire_timeout
    async with timeout(2):
        async with pool_manager.acquire_replica(
            fallback_master=True,
            timeout=2,
        ) as conn:
            assert await conn.is_master()


async def test_round_robin_master_with_fallback_and_no_replicas(
    make_pool_manager,
):
    pool_manager = await make_pool_manager(
        RoundRobinBalancerPolicy,
        replicas_count=0,
    )
    async with timeout(1):
        await pool_manager._pool_state.ready()

    assert pool_manager._pool_state.replica_pool_count == 0

    # Acquiring master should work even when fallback_master
    # is set and there are no replicas
    async with timeout(1):
        async with pool_manager.acquire_master() as conn:
            assert await conn.is_master()


@balancer_policies
async def test_read_only_selects_weighted_master_without_replicas(
    make_pool_manager,
    balancer_policy,
):
    pool_manager = await make_pool_manager(balancer_policy, replicas_count=0)
    async with timeout(1):
        await pool_manager.ready()

    pool = await asyncio.wait_for(
        pool_manager._balancer.get_pool(
            read_only=True,
            fallback_master=False,
            master_as_replica_weight=1.0,
        ),
        timeout=0.2,
    )

    assert pool_manager._pool_state.pool_is_master(pool)
