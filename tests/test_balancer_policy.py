import asyncio

import pytest
from async_timeout import timeout

from hasql.balancer_policy import (
    GreedyBalancerPolicy,
    RandomWeightedBalancerPolicy,
    RoundRobinBalancerPolicy,
)
from tests.mocks import TestPoolManager
from tests.mocks.pool_manager import TestPool

balancer_policies = pytest.mark.parametrize(
    "balancer_policy",
    [
        GreedyBalancerPolicy,
        RandomWeightedBalancerPolicy,
        RoundRobinBalancerPolicy,
    ],
)


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
async def test_get_pool_write_with_master_as_replica_weight_raises(
    make_pool_manager,
    balancer_policy,
):
    pool_manager = await make_pool_manager(balancer_policy)
    async with timeout(1):
        await pool_manager.pool_state.ready()
    with pytest.raises(ValueError, match="master_as_replica_weight"):
        await pool_manager.balancer.get_pool(
            read_only=False,
            master_as_replica_weight=0.5,
        )


def test_random_weighted_compute_weights_equal_times():
    # Equal response times should produce equal weights
    weights = RandomWeightedBalancerPolicy._compute_weights([0.1, 0.1, 0.1])
    assert len(weights) == 3
    assert weights[0] == pytest.approx(weights[1])
    assert weights[1] == pytest.approx(weights[2])


def test_random_weighted_compute_weights_none_times():
    # None times treated as 0 — all equal
    weights = RandomWeightedBalancerPolicy._compute_weights([None, None])
    assert len(weights) == 2
    assert weights[0] == pytest.approx(weights[1])


def test_random_weighted_compute_weights_all_none_produces_uniform():
    # When all response times are None (e.g. at startup before any health
    # checks), weights should be uniform and all positive.
    for n in (1, 2, 3, 5, 10):
        weights = RandomWeightedBalancerPolicy._compute_weights(
            [None] * n,
        )
        assert len(weights) == n
        assert all(w > 0 for w in weights)
        assert all(w == pytest.approx(weights[0]) for w in weights)


def test_random_weighted_compute_weights_all_zero_produces_uniform():
    # Explicit zero times should also produce uniform positive weights
    weights = RandomWeightedBalancerPolicy._compute_weights([0, 0, 0])
    assert len(weights) == 3
    assert all(w > 0 for w in weights)
    assert all(w == pytest.approx(weights[0]) for w in weights)


def test_random_weighted_compute_weights_favors_faster():
    # Faster pool (lower time) should get higher weight
    weights = RandomWeightedBalancerPolicy._compute_weights([0.1, 0.9])
    assert weights[0] > weights[1]


async def test_round_robin_master_as_replica(make_pool_manager):
    pool_manager = await make_pool_manager(
        RoundRobinBalancerPolicy,
        replicas_count=0,
    )
    async with timeout(1):
        await pool_manager.pool_state.ready()

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
        await pool_manager.pool_state.ready()

    # Shut down the master so master_pool_count drops to 0
    ps = pool_manager.pool_state
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
        await pool_manager.pool_state.ready()

    # Shut down all replicas so replica_pool_count drops to 0
    ps = pool_manager.pool_state
    replica_pools = [
        pool
        for pool in ps.pools
        if pool is not None and ps.pool_is_replica(pool)
    ]
    for rp in replica_pools:
        rp.shutdown()

    # Wait for health monitor to detect shutdowns
    await pool_manager.pool_state.wait_next_pool_check()
    assert pool_manager.pool_state.replica_pool_count == 0

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
        await pool_manager.pool_state.ready()

    # Shut down the master so both master and replica counts are 0
    ps = pool_manager.pool_state
    master_pool: TestPool = (await ps.get_master_pools())[0]
    master_pool.shutdown()

    await pool_manager.pool_state.wait_next_pool_check()
    assert pool_manager.pool_state.master_pool_count == 0
    assert pool_manager.pool_state.replica_pool_count == 0

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
        await pool_manager.pool_state.ready()

    assert pool_manager.pool_state.replica_pool_count == 0

    # Acquiring master should work even when fallback_master
    # is set and there are no replicas
    async with timeout(1):
        async with pool_manager.acquire_master() as conn:
            assert await conn.is_master()
