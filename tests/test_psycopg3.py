import asyncio
from contextlib import AsyncExitStack

import mock
import pytest
pytest.importorskip("psycopg")
pytest.importorskip("psycopg_pool")
from psycopg import AsyncConnection, errors
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from psycopg_pool import PoolTimeout, TooManyRequests

from hasql.driver.psycopg3 import PoolManager, Psycopg3Driver


@pytest.fixture
def pool_size() -> int:
    return 10


@pytest.fixture
async def pool_manager(pg_dsn, pool_size):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        acquire_timeout=1,
        pool_factory_kwargs={"min_size": pool_size, "max_size": pool_size},
    )
    try:
        await pg_pool._pool_state.ready()
        yield pg_pool
    finally:
        await pg_pool.close()


async def test_acquire_with_context(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert isinstance(conn, AsyncConnection)
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            assert await cursor.fetchall() == [(1,)]


async def test_acquire_without_context(pool_manager):
    conn = await pool_manager.acquire_master()
    assert isinstance(conn, AsyncConnection)
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        assert await cursor.fetchall() == [(1,)]


async def test_close(pool_manager):
    aiopg_pool = await pool_manager._balancer.get_pool(read_only=False)
    await pool_manager.close()
    assert aiopg_pool.closed


async def test_release(pool_manager):
    aiopg_pool = await pool_manager._balancer.get_pool(read_only=False)
    assert pool_manager._pool_state.get_pool_freesize(aiopg_pool) == 10
    async with pool_manager.acquire_master() as _conn:
        assert pool_manager._pool_state.get_pool_freesize(aiopg_pool) == 9
    assert pool_manager._pool_state.get_pool_freesize(aiopg_pool) == 10


async def test_is_connection_closed(pool_manager):
    async with pool_manager.acquire_master() as conn:
        assert not pool_manager._pool_state.is_connection_closed(conn)
        await conn.close()
        assert pool_manager._pool_state.is_connection_closed(conn)


async def test_acquire_with_timeout_context(pool_manager, pool_size):
    conns = []
    for _ in range(pool_size):
        conns.append(await pool_manager.acquire_master())

    with pytest.raises(PoolTimeout):
        await pool_manager.acquire_master()

    for conn in conns:
        pool = pool_manager._unmanaged_connections.pop(conn, None)
        if pool is not None:
            await pool_manager._pool_state.release_to_pool(conn, pool)
    conns.clear()

    for pool in pool_manager._pool_state.pools:
        assert pool_manager._pool_state.get_pool_freesize(pool) == pool_size

    for _ in range(pool_size):
        async with pool_manager.acquire_master() as conn:
            pass


@pytest.fixture
async def queue_limited_pool_manager(pg_dsn, pool_size):
    pg_pool = PoolManager(
        dsn=pg_dsn,
        fallback_master=True,
        acquire_timeout=1,
        pool_factory_kwargs={
            "min_size": pool_size,
            "max_size": pool_size,
            "max_waiting": 1,
        },
    )
    try:
        await pg_pool._pool_state.ready()
        yield pg_pool
    finally:
        await pg_pool.close()


async def test_acquire_with_queue_limit(queue_limited_pool_manager, pool_size):
    async with AsyncExitStack() as stack:
        for _ in range(pool_size):
            await stack.enter_async_context(
                queue_limited_pool_manager.acquire_master(),
            )

        async def wait_for_connection():
            async with queue_limited_pool_manager.acquire_master():
                pass

        waiter = asyncio.create_task(wait_for_connection())
        await asyncio.sleep(0.1)

        with pytest.raises(TooManyRequests):
            await queue_limited_pool_manager.acquire_master()

        assert not waiter.done()

    await waiter


def test_acquire_from_pool_passes_timeout():
    from hasql.driver.psycopg3 import Psycopg3AcquireContext, Psycopg3Driver

    driver = Psycopg3Driver()
    pool = mock.MagicMock()
    ctx = driver.acquire_from_pool(pool, timeout=0.25)
    assert isinstance(ctx, Psycopg3AcquireContext)
    assert ctx.timeout == 0.25


async def test_metrics(pool_manager):
    async with pool_manager.acquire_master():
        pools = pool_manager.metrics().pools
        assert len(pools) == 1
        p = pools[0]
        assert p.max == 11
        assert p.min == 11
        assert p.idle == 9
        assert p.used == 2
        assert p.role == "master"
        assert p.healthy is True
        assert p.in_flight == 1
        assert "pool_size" in p.extra


async def _role_probe(driver, connection):
    return await driver.is_master(connection)


async def _scalar_probe(driver, connection):
    return await driver.fetch_scalar(connection, "SELECT 1")


_PROBES = [
    pytest.param(_role_probe, True, id="is-master"),
    pytest.param(_scalar_probe, 1, id="fetch-scalar"),
]


@pytest.mark.parametrize("probe,expected", _PROBES)
@pytest.mark.parametrize("autocommit", [False, True])
async def test_health_probe_finishes_its_own_transaction(
    pg_dsn, probe, expected, autocommit,
):
    driver = Psycopg3Driver()
    async with await AsyncConnection.connect(
        pg_dsn, autocommit=autocommit,
    ) as connection:
        result = await probe(driver, connection)
        status = connection.info.transaction_status

    assert (result, status) == (expected, TransactionStatus.IDLE)


@pytest.mark.parametrize("autocommit", [False, True])
async def test_scalar_probe_error_rolls_back_owned_transaction(
    pg_dsn, autocommit,
):
    driver = Psycopg3Driver()
    async with await AsyncConnection.connect(
        pg_dsn, autocommit=autocommit,
    ) as connection:
        with pytest.raises(errors.DivisionByZero):
            await driver.fetch_scalar(connection, "SELECT 1 / 0")
        status_after_error = connection.info.transaction_status
        result = await driver.fetch_scalar(connection, "SELECT 1")
        status_after_reuse = connection.info.transaction_status

    assert (status_after_error, result, status_after_reuse) == (
        TransactionStatus.IDLE, 1, TransactionStatus.IDLE,
    )


def _failing_row_factory(cursor):
    # Exercise a real SHOW/cursor fetch failure via psycopg's public row-factory
    # hook, without replacing the connection, cursor, driver, or SQL transport.
    def fail_row(values):
        raise ValueError("probe row conversion failed")

    return fail_row


@pytest.mark.parametrize("autocommit", [False, True])
async def test_role_probe_row_error_rolls_back_owned_transaction(
    pg_dsn, autocommit,
):
    driver = Psycopg3Driver()
    async with await AsyncConnection.connect(
        pg_dsn, autocommit=autocommit, row_factory=_failing_row_factory,
    ) as connection:
        with pytest.raises(ValueError, match="probe row conversion failed"):
            await driver.is_master(connection)
        status_after_error = connection.info.transaction_status
        connection.row_factory = tuple_row
        result = await driver.is_master(connection)
        status_after_reuse = connection.info.transaction_status

    assert (status_after_error, result, status_after_reuse) == (
        TransactionStatus.IDLE, True, TransactionStatus.IDLE,
    )


async def _set_caller_marker(connection):
    # Transaction-local GUC, not persistent test data. A driver-wide commit or
    # rollback would discard it and violate caller transaction ownership.
    await connection.execute(
        "SELECT set_config('hasql.probe_marker', 'caller-owned', true)",
    )


async def _caller_marker(connection):
    cursor = await connection.execute(
        "SELECT current_setting('hasql.probe_marker')",
    )
    return (await cursor.fetchone())[0]


@pytest.mark.parametrize("probe,expected", _PROBES)
@pytest.mark.parametrize("autocommit", [False, True])
async def test_health_probe_preserves_uncommitted_caller_transaction(
    pg_dsn, probe, expected, autocommit,
):
    driver = Psycopg3Driver()
    async with await AsyncConnection.connect(
        pg_dsn, autocommit=autocommit,
    ) as connection:
        async with connection.transaction(force_rollback=True):
            await _set_caller_marker(connection)
            result = await probe(driver, connection)
            status = connection.info.transaction_status
            marker = await _caller_marker(connection)

    assert (result, status, marker) == (
        expected, TransactionStatus.INTRANS, "caller-owned",
    )


@pytest.mark.parametrize("autocommit", [False, True])
async def test_scalar_probe_error_preserves_usable_caller_transaction(
    pg_dsn, autocommit,
):
    driver = Psycopg3Driver()
    async with await AsyncConnection.connect(
        pg_dsn, autocommit=autocommit,
    ) as connection:
        async with connection.transaction(force_rollback=True):
            await _set_caller_marker(connection)
            with pytest.raises(errors.DivisionByZero):
                await driver.fetch_scalar(connection, "SELECT 1 / 0")
            status = connection.info.transaction_status
            marker = await _caller_marker(connection)

    assert (status, marker) == (TransactionStatus.INTRANS, "caller-owned")


@pytest.mark.parametrize("autocommit", [False, True])
async def test_role_probe_row_error_preserves_usable_caller_transaction(
    pg_dsn, autocommit,
):
    driver = Psycopg3Driver()
    async with await AsyncConnection.connect(
        pg_dsn, autocommit=autocommit,
    ) as connection:
        async with connection.transaction(force_rollback=True):
            await _set_caller_marker(connection)
            connection.row_factory = _failing_row_factory
            with pytest.raises(ValueError, match="probe row conversion failed"):
                await driver.is_master(connection)
            status = connection.info.transaction_status
            connection.row_factory = tuple_row
            marker = await _caller_marker(connection)

    assert (status, marker) == (TransactionStatus.INTRANS, "caller-owned")
