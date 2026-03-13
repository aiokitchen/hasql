import asyncio
from asyncio import CancelledError
from contextlib import ExitStack
from typing import Optional
from unittest.mock import patch, AsyncMock

import pytest
from async_timeout import timeout as timeout_context

from hasql.base import BasePoolManager
from tests.mocks import TestPoolManager


@pytest.fixture
def dsn():
    return "postgresql://test:test@master,replica1,replica2/test"


@pytest.fixture
async def pool_manager(dsn):
    pool_manager = TestPoolManager(dsn, refresh_timeout=0.2, refresh_delay=0.1)
    try:
        yield pool_manager
    finally:
        await pool_manager.close()


def pool_is_master(pool_manager: BasePoolManager, pool):
    assert pool_manager.pool_is_master(pool)
    assert not pool_manager.pool_is_replica(pool)


def pool_is_replica(pool_manager: BasePoolManager, pool):
    assert pool_manager.pool_is_replica(pool)
    assert not pool_manager.pool_is_master(pool)


async def test_wait_next_pool_check(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    master_pool.shutdown()
    assert pool_manager.master_pool_count == 1
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0


async def test_ready_all_hosts(pool_manager: BasePoolManager):
    await pool_manager.ready()
    assert len(pool_manager.dsn) == pool_manager.available_pool_count


async def test_ready_min_count_hosts(pool_manager: BasePoolManager):
    await pool_manager.ready()
    replica_pools = await pool_manager.get_replica_pools()
    for replica_pool in replica_pools:
        replica_pool.shutdown()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    master_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0
    assert pool_manager.replica_pool_count == 0
    master_pool.startup()
    master_pool.set_master(True)
    await pool_manager.ready(masters_count=1, replicas_count=0)
    assert pool_manager.master_pool_count == 1
    assert pool_manager.replica_pool_count == 0


@pytest.mark.parametrize(
    ["masters_count", "replicas_count"],
    [
        [-1, 5],
        [2, -10],
        [1, None],
        [None, 2],
    ],
)
async def test_ready_with_invalid_arguments(
    pool_manager: BasePoolManager,
    masters_count: Optional[int],
    replicas_count: Optional[int],
):
    with pytest.raises(ValueError):
        await pool_manager.ready(masters_count, replicas_count)


async def test_wait_db_restart(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    assert pool_manager.pool_is_master(master_pool)
    master_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0
    master_pool.startup()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0
    assert pool_manager.pool_is_replica(master_pool)


async def test_master_shutdown(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    assert pool_manager.pool_is_master(master_pool)
    master_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0


async def test_replica_shutdown(pool_manager: BasePoolManager):
    await pool_manager.ready()
    replica_pool = await pool_manager.balancer.get_pool(read_only=True)
    assert pool_manager.pool_is_replica(replica_pool)
    assert pool_manager.replica_pool_count == 2
    replica_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.replica_pool_count == 1


async def test_change_master(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    replica_pool = await pool_manager.balancer.get_pool(read_only=True)
    pool_is_master(pool_manager, master_pool)
    pool_is_replica(pool_manager, replica_pool)
    master_pool.set_master(False)
    replica_pool.set_master(True)
    await pool_manager.wait_next_pool_check()
    pool_is_master(pool_manager, replica_pool)
    pool_is_replica(pool_manager, master_pool)


async def test_define_roles(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    replica_pool = await pool_manager.balancer.get_pool(read_only=True)
    pool_is_master(pool_manager, master_pool)
    pool_is_replica(pool_manager, replica_pool)


async def test_acquire_master_and_release(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    init_freesize = pool_manager.get_pool_freesize(master_pool)
    connection = await pool_manager.acquire_master()
    assert pool_manager.get_pool_freesize(master_pool) + 1 == init_freesize
    assert connection in master_pool.used
    await pool_manager.release(connection)
    assert connection not in master_pool.used
    assert pool_manager.get_pool_freesize(master_pool) == init_freesize


async def test_acquire_with_context(pool_manager: BasePoolManager):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    init_freesize = pool_manager.get_pool_freesize(master_pool)
    async with pool_manager.acquire_master() as connection:
        assert pool_manager.get_pool_freesize(master_pool) + 1 == init_freesize
        assert connection in master_pool.used
    assert connection not in master_pool.used
    assert pool_manager.get_pool_freesize(master_pool) == init_freesize


async def test_acquire_replica_with_fallback_master_is_true(
    pool_manager: BasePoolManager,
):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)
    replica_pools = await pool_manager.get_replica_pools()
    for replica_pool in replica_pools:
        assert pool_manager.pool_is_replica(replica_pool)
        replica_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.replica_pool_count == 0
    async with timeout_context(1):
        async with pool_manager.acquire_replica(
            fallback_master=True,
        ) as connection:
            assert connection in master_pool.used


async def test_acquire_replica_with_fallback_master_is_false(
    pool_manager: BasePoolManager,
):
    await pool_manager.ready()
    replica_pools = await pool_manager.get_replica_pools()
    for replica_pool in replica_pools:
        assert pool_manager.pool_is_replica(replica_pool)
        replica_pool.shutdown()
    await pool_manager.wait_next_pool_check()
    assert pool_manager.replica_pool_count == 0
    with pytest.raises(asyncio.TimeoutError):
        async with timeout_context(1):
            await pool_manager.acquire_replica(fallback_master=False)


async def test_close(pool_manager: BasePoolManager):
    await pool_manager.ready()
    assert pool_manager.master_pool_count > 0
    assert pool_manager.replica_pool_count > 0
    await pool_manager.close()
    assert pool_manager.master_pool_count == 0
    assert pool_manager.replica_pool_count == 0
    for pool in pool_manager:
        assert pool is not None
        assert all(
            pool_manager.is_connection_closed(conn) for conn in pool.connections
        )
        assert all(conn.close.call_count == 1 for conn in pool.connections)


async def test_terminate(pool_manager: BasePoolManager):
    await pool_manager.ready()
    assert pool_manager.master_pool_count > 0
    assert pool_manager.replica_pool_count > 0
    await pool_manager.terminate()
    assert pool_manager.master_pool_count == 0
    assert pool_manager.replica_pool_count == 0
    for pool in pool_manager:
        assert pool is not None
        assert all(
            pool_manager.is_connection_closed(conn) for conn in pool.connections
        )
        assert all(conn.terminate.call_count == 1 for conn in pool.connections)


async def test_master_behind_firewall(pool_manager: BasePoolManager):
    await pool_manager.ready()
    assert pool_manager.master_pool_count == 1
    master_pool = (await pool_manager.get_master_pools())[0]
    master_pool.behind_firewall(True)
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 0
    master_pool.behind_firewall(False)
    await pool_manager.wait_next_pool_check()
    assert pool_manager.master_pool_count == 1


async def test_replica_behind_firewall(pool_manager: BasePoolManager):
    await pool_manager.ready()
    replica_pool_count = 2
    assert pool_manager.replica_pool_count == replica_pool_count
    replica_pools = await pool_manager.get_replica_pools()
    for replica_pool in replica_pools:
        replica_pool.behind_firewall(True)
        await pool_manager.wait_next_pool_check()
        assert pool_manager.replica_pool_count == replica_pool_count - 1
        replica_pool.behind_firewall(False)
        await pool_manager.wait_next_pool_check()
        assert pool_manager.replica_pool_count == replica_pool_count


async def test_check_pool_canceled_error_while_releasing_connection(
    pool_manager: BasePoolManager
):
    await pool_manager.ready()
    master_pool = await pool_manager.balancer.get_pool(read_only=False)

    with ExitStack() as stack:
        for conn in master_pool.connections:
            stack.enter_context(
                patch.object(
                    conn, 'is_master', AsyncMock(side_effect=Exception)
                )
            )
        stack.enter_context(
            patch.object(
                master_pool, 'release', AsyncMock(side_effect=CancelledError)
            )
        )
        await asyncio.sleep(1)
        for task in pool_manager._health.tasks:
            assert not task.done()


def test_invalid_balancer_policy():
    with pytest.raises(ValueError, match="balancer_policy"):
        TestPoolManager(
            dsn="postgresql://test:test@master/test",
            balancer_policy=str,
        )


async def test_release_unknown_connection(pool_manager: BasePoolManager):
    await pool_manager.ready()
    fake_conn = object()
    with pytest.raises(ValueError, match="is not a member of this pool"):
        await pool_manager.release(fake_conn)


async def test_acquire_master_as_replica_weight_write_raises(
    pool_manager: BasePoolManager,
):
    await pool_manager.ready()
    with pytest.raises(ValueError, match="master_as_replica_weight"):
        pool_manager.acquire(read_only=False, master_as_replica_weight=0.5)


@pytest.mark.parametrize("weight", [-0.1, 1.1, 2.0])
async def test_acquire_master_as_replica_weight_out_of_range(
    pool_manager: BasePoolManager,
    weight: float,
):
    await pool_manager.ready()
    with pytest.raises(ValueError, match="segment"):
        pool_manager.acquire(read_only=True, master_as_replica_weight=weight)


async def test_properties(pool_manager: BasePoolManager):
    await pool_manager.ready()
    assert pool_manager.refresh_delay == 0.1
    assert pool_manager.refresh_timeout == 0.2
    assert pool_manager.pool_factory_kwargs is not None
    assert pool_manager.closing is False
    assert pool_manager.closed is False


async def test_metrics_after_acquire(pool_manager: BasePoolManager):
    await pool_manager.ready()
    conn = await pool_manager.acquire_master()
    from hasql.metrics import Metrics
    m = pool_manager.metrics()
    assert isinstance(m, Metrics)
    assert m.hasql.pool == 1
    assert m.hasql.add_connections.get("test-host:5432") == 1
    await pool_manager.release(conn)


async def test_aenter_aexit(dsn):
    async with TestPoolManager(
        dsn, refresh_timeout=0.2, refresh_delay=0.1,
    ) as pm:
        assert pm.master_pool_count > 0
    assert pm.closed


async def test_terminate_skips_none_pools(pool_manager: BasePoolManager):
    await pool_manager.ready()
    # Force a None entry into the pools list
    pool_manager._pools.append(None)
    await pool_manager.terminate()
    assert pool_manager.closed


async def test_close_releases_unmanaged_connections(
    pool_manager: BasePoolManager,
):
    await pool_manager.ready()
    conn = await pool_manager.acquire_master()
    assert conn in pool_manager._unmanaged_connections
    await pool_manager.close()
    assert pool_manager.closed
    assert len(pool_manager._unmanaged_connections) == 0


async def test_check_pool_task_cancelled_error_non_closing():
    """CancelledError during _is_master when not closing removes pool."""
    pool_manager = TestPoolManager(
        "postgresql://test:test@master/test",
        refresh_timeout=0.2,
        refresh_delay=0.05,
    )
    try:
        await pool_manager.ready()
        assert pool_manager.master_pool_count == 1

        with patch.object(
            pool_manager,
            '_is_master',
            AsyncMock(side_effect=asyncio.CancelledError()),
        ):
            await pool_manager.wait_next_pool_check()
            assert pool_manager.master_pool_count == 0

        # Recovers after the patch is removed
        await pool_manager.wait_next_pool_check()
        assert pool_manager.master_pool_count == 1
    finally:
        await pool_manager.close()


async def test_wait_creating_pool_retries_on_failure():
    """_wait_creating_pool retries when _pool_factory raises."""
    call_count = 0
    original_pool_factory = TestPoolManager._pool_factory

    async def failing_factory(self, dsn):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("cannot connect")
        return await original_pool_factory(self, dsn)

    with patch.object(
        TestPoolManager, '_pool_factory', failing_factory,
    ):
        pm = TestPoolManager(
            "postgresql://test:test@master/test",
            refresh_timeout=0.2,
            refresh_delay=0.05,
        )
        try:
            await pm.ready(timeout=5)
            assert call_count >= 3
            assert pm.master_pool_count == 1
        finally:
            await pm.close()


async def test_check_pool_task_release_exception():
    """Exception during release_to_pool in _check_pool_task is handled."""
    pool_manager = TestPoolManager(
        "postgresql://test:test@master/test",
        refresh_timeout=0.2,
        refresh_delay=0.05,
    )
    try:
        await pool_manager.ready()
        assert pool_manager.master_pool_count == 1

        master_pool = (await pool_manager.get_master_pools())[0]

        with ExitStack() as stack:
            for conn in master_pool.connections:
                stack.enter_context(
                    patch.object(
                        conn, 'is_master',
                        AsyncMock(side_effect=Exception("db error")),
                    )
                )
            stack.enter_context(
                patch.object(
                    master_pool, 'release',
                    AsyncMock(side_effect=RuntimeError("release failed")),
                )
            )
            await asyncio.sleep(0.5)
            # Tasks should still be running despite release errors
            for task in pool_manager._health.tasks:
                assert not task.done()
    finally:
        await pool_manager.close()
