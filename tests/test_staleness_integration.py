import pytest

from tests.mocks.pool_manager import TestPoolManager


@pytest.fixture
def make_dsn():
    def factory(replicas=2):
        hosts = ",".join(
            ["master"] + [f"replica{i}" for i in range(replicas)],
        )
        return f"postgresql://test:test@{hosts}:5432/test"
    return factory


@pytest.fixture
async def pool_manager(make_dsn):
    pm = TestPoolManager(make_dsn(replicas=2))
    await pm.ready(masters_count=1, replicas_count=2)
    yield pm
    await pm.close()


async def test_pool_state_stale_pool_set_empty_by_default(pool_manager):
    ps = pool_manager._pool_state
    assert ps.stale_pool_count == 0
    assert ps.get_stale_pools() == []


async def test_pool_state_stale_pools_in_iter(pool_manager):
    ps = pool_manager._pool_state
    all_pools = list(ps)
    # master + 2 replicas, no stale
    assert len(all_pools) == 3


async def test_pool_state_available_pool_count_includes_stale(pool_manager):
    ps = pool_manager._pool_state
    # Move one replica to stale set
    replica_pools = list(ps._replica_pool_set)
    stale_pool = replica_pools[0]
    ps._replica_pool_set.discard(stale_pool)
    ps._stale_pool_set.add(stale_pool)

    assert ps.stale_pool_count == 1
    assert ps.replica_pool_count == 1
    assert ps.available_pool_count == 3  # 1 master + 1 replica + 1 stale


async def test_balancer_prefers_fresh_over_stale():
    """When fresh replicas exist, stale replicas are not selected."""
    dsn = "postgresql://test:test@master,replica0,replica1:5432/test"
    pm = TestPoolManager(dsn)
    await pm.ready(masters_count=1, replicas_count=2)

    ps = pm._pool_state
    # Move one replica to stale set manually
    replica_pools = list(ps._replica_pool_set)
    stale_pool = replica_pools[0]
    fresh_pool = replica_pools[1]
    ps._replica_pool_set.discard(stale_pool)
    ps._stale_pool_set.add(stale_pool)

    # Acquire should get the fresh pool
    async with pm.acquire_replica() as conn:
        assert conn._pool is fresh_pool

    await pm.close()


async def test_balancer_falls_back_to_stale():
    """When no fresh replicas or masters, falls back to stale."""
    dsn = "postgresql://test:test@master,replica0:5432/test"
    pm = TestPoolManager(dsn)
    await pm.ready(masters_count=1, replicas_count=1)

    ps = pm._pool_state
    # Move all replicas to stale
    replica_pools = list(ps._replica_pool_set)
    for pool in replica_pools:
        ps._replica_pool_set.discard(pool)
        ps._stale_pool_set.add(pool)

    # Also remove master to test stale-only fallback
    master_pools = list(ps._master_pool_set)
    for pool in master_pools:
        ps._master_pool_set.discard(pool)

    # Should fall back to stale
    async with pm.acquire_replica() as conn:
        assert conn._pool in ps._stale_pool_set

    await pm.close()
