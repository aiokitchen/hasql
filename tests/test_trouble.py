import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from importlib import import_module
from unittest import mock

import pytest
from async_timeout import timeout
import sqlalchemy as sa

from hasql.exceptions import PoolManagerClosedError
from hasql.staleness import (
    BytesStalenessChecker,
    CheckContext,
    StalenessCheckResult,
    TimeStalenessChecker,
)

from tests.conftest import (
    setup_aiopg,
    setup_aiopgsa,
    setup_asyncpg,
    setup_asyncsqlalchemy,
    setup_psycopg3,
)


@pytest.mark.parametrize(
    "pool_manager_factory",
    [
        (setup_aiopg),
        (setup_aiopgsa),
        (setup_asyncpg),
        (setup_asyncsqlalchemy),
        (setup_psycopg3),
    ],
)
async def test_unavailable_db(pool_manager_factory, localhost, db_server_port):
    async with timeout(1):
        pg_dsn = f"postgres://pg:pg@{localhost}:{db_server_port}/pg"
        async with pool_manager_factory(pg_dsn):
            pass


_AIOSQA = "hasql.driver.asyncsqlalchemy.AsyncSqlAlchemyDriver"


@pytest.mark.parametrize(
    "pool_manager_factory,driver_class",
    [
        (setup_aiopg, "hasql.driver.aiopg.AiopgDriver"),
        (setup_aiopgsa, "hasql.driver.aiopg_sa.AiopgSaDriver"),
        (setup_asyncpg, "hasql.driver.asyncpg.AsyncpgDriver"),
        (setup_asyncsqlalchemy, _AIOSQA),
        (setup_psycopg3, "hasql.driver.psycopg3.Psycopg3Driver"),
    ],
)
async def test_catch_cancelled_error(
    pool_manager_factory, pg_dsn, driver_class,
):
    async with pool_manager_factory(pg_dsn) as pool_manager:
        await pool_manager._pool_state.ready()
        assert pool_manager._pool_state.available_pool_count > 0
        with mock.patch(
            f"{driver_class}.is_master",
            side_effect=asyncio.CancelledError(),
        ):
            await pool_manager._pool_state.wait_next_pool_check()
            assert pool_manager._pool_state.available_pool_count == 0
        await pool_manager._pool_state.wait_next_pool_check()
        assert pool_manager._pool_state.available_pool_count > 0


_SETUP_ADAPTERS = [
    pytest.param(setup_aiopg, "aiopg", id="aiopg"),
    pytest.param(setup_aiopgsa, "aiopg_sa", id="aiopg-sa"),
    pytest.param(setup_asyncpg, "asyncpg", id="asyncpg"),
    pytest.param(
        setup_asyncsqlalchemy, "asyncsqlalchemy", id="asyncsqlalchemy",
    ),
    pytest.param(setup_psycopg3, "psycopg3", id="psycopg3"),
]


class _OwnedManager:
    """No driver emulation: only observes setup-helper resource ownership."""

    closed = False

    async def close(self):
        self.closed = True


@pytest.mark.parametrize("setup_manager,adapter", _SETUP_ADAPTERS)
async def test_setup_helper_closes_manager_on_body_exception(
    setup_manager, adapter, monkeypatch,
):
    manager = _OwnedManager()
    monkeypatch.setattr(
        f"hasql.driver.{adapter}.PoolManager", lambda **kwargs: manager,
    )

    with pytest.raises(RuntimeError, match="body failed"):
        async with setup_manager("unused-dsn"):
            raise RuntimeError("body failed")

    assert manager.closed is True


@pytest.mark.parametrize("setup_manager,adapter", _SETUP_ADAPTERS)
async def test_setup_helper_closes_manager_on_body_cancellation(
    setup_manager, adapter, monkeypatch,
):
    manager = _OwnedManager()
    monkeypatch.setattr(
        f"hasql.driver.{adapter}.PoolManager", lambda **kwargs: manager,
    )
    entered = asyncio.Event()

    async def use_manager():
        async with setup_manager("unused-dsn"):
            entered.set()
            await asyncio.Future()

    task = asyncio.create_task(use_manager())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert manager.closed is True


async def _cursor_scalar(connection, query):
    async with connection.cursor() as cursor:
        await cursor.execute(query)
        row = await cursor.fetchone()
        return row[0]


async def _aiopg_sa_scalar(connection, query):
    return await connection.scalar(query)


async def _asyncpg_scalar(connection, query):
    return await connection.fetchval(query)


async def _asyncsqlalchemy_scalar(connection, query):
    return await connection.scalar(sa.text(query))


_SMOKE_ADAPTERS = [
    pytest.param(
        "aiopg", _cursor_scalar, {"minsize": 1, "maxsize": 1}, id="aiopg",
    ),
    pytest.param(
        "aiopg_sa", _aiopg_sa_scalar, {"minsize": 1, "maxsize": 1},
        id="aiopg-sa",
    ),
    pytest.param(
        "asyncpg", _asyncpg_scalar, {"min_size": 1, "max_size": 1},
        id="asyncpg",
    ),
    pytest.param(
        "asyncpgsa", _asyncpg_scalar, {"min_size": 1, "max_size": 1},
        id="asyncpgsa",
    ),
    pytest.param(
        "asyncsqlalchemy", _asyncsqlalchemy_scalar,
        {"pool_size": 1, "max_overflow": 0}, id="asyncsqlalchemy",
    ),
    pytest.param(
        "psycopg3", _cursor_scalar, {"min_size": 1, "max_size": 1},
        id="psycopg3",
    ),
]


@asynccontextmanager
async def _smoke_manager(adapter, pg_dsn, pool_kwargs):
    # Drivers reserve an additional health-check connection. One application
    # slot makes reacquisition fail deterministically if release leaks it.
    manager = import_module(f"hasql.driver.{adapter}").PoolManager(
        pg_dsn, fallback_master=True, acquire_timeout=1,
        pool_factory_kwargs=pool_kwargs,
    )
    try:
        await manager.ready(masters_count=1, replicas_count=0, timeout=3)
        yield manager
    finally:
        await manager.close()


@pytest.mark.parametrize("adapter,scalar,pool_kwargs", _SMOKE_ADAPTERS)
async def test_real_pg_native_scalar_with_context(
    adapter, scalar, pool_kwargs, pg_dsn,
):
    async with _smoke_manager(adapter, pg_dsn, pool_kwargs) as manager:
        async with manager.acquire_master() as connection:
            result = await scalar(connection, "SELECT 1")

    assert result == 1


@pytest.mark.parametrize("adapter,scalar,pool_kwargs", _SMOKE_ADAPTERS)
async def test_real_pg_noncontext_release_allows_reacquisition(
    adapter, scalar, pool_kwargs, pg_dsn,
):
    async with _smoke_manager(adapter, pg_dsn, pool_kwargs) as manager:
        connection = await manager.acquire_master()
        first = await scalar(connection, "SELECT 1")
        await manager.release(connection)
        reacquired = await manager.acquire_master()
        second = await scalar(reacquired, "SELECT 2")
        await manager.release(reacquired)

    assert (first, second) == (1, 2)


@pytest.mark.parametrize("adapter,scalar,pool_kwargs", _SMOKE_ADAPTERS)
async def test_real_pg_body_exception_releases_contextual_connection(
    adapter, scalar, pool_kwargs, pg_dsn,
):
    async with _smoke_manager(adapter, pg_dsn, pool_kwargs) as manager:
        with pytest.raises(RuntimeError, match="body failed"):
            async with manager.acquire_master() as connection:
                await scalar(connection, "SELECT 1")
                raise RuntimeError("body failed")
        async with manager.acquire_master() as reacquired:
            result = await scalar(reacquired, "SELECT 2")

    assert result == 2


@pytest.mark.parametrize("adapter,scalar,pool_kwargs", _SMOKE_ADAPTERS)
async def test_real_pg_body_cancellation_releases_contextual_connection(
    adapter, scalar, pool_kwargs, pg_dsn,
):
    entered = asyncio.Event()
    async with _smoke_manager(adapter, pg_dsn, pool_kwargs) as manager:
        async def use_connection():
            async with manager.acquire_master() as connection:
                await scalar(connection, "SELECT 1")
                entered.set()
                await asyncio.Future()

        task = asyncio.create_task(use_connection())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with manager.acquire_master() as reacquired:
            result = await scalar(reacquired, "SELECT 2")

    assert result == 2


@pytest.mark.parametrize("adapter,scalar,pool_kwargs", _SMOKE_ADAPTERS)
async def test_real_pg_graceful_close_after_release(
    adapter, scalar, pool_kwargs, pg_dsn,
):
    async with _smoke_manager(adapter, pg_dsn, pool_kwargs) as manager:
        connection = await manager.acquire_master()
        await scalar(connection, "SELECT 1")
        await manager.release(connection)
        await asyncio.wait_for(manager.close(), timeout=2)

    assert manager.metrics().gauges.closed is True


@pytest.mark.parametrize("adapter,scalar,pool_kwargs", _SMOKE_ADAPTERS)
async def test_real_pg_acquisition_rejected_after_close(
    adapter, scalar, pool_kwargs, pg_dsn,
):
    async with _smoke_manager(adapter, pg_dsn, pool_kwargs) as manager:
        async with manager.acquire_master() as connection:
            await scalar(connection, "SELECT 1")
        await manager.close()

        with pytest.raises(PoolManagerClosedError):
            await manager.acquire_master()


@pytest.mark.parametrize("adapter,scalar,pool_kwargs", _SMOKE_ADAPTERS)
async def test_real_pg_replica_read_falls_back_to_master(
    adapter, scalar, pool_kwargs, pg_dsn,
):
    async with _smoke_manager(adapter, pg_dsn, pool_kwargs) as manager:
        async with manager.acquire_replica(
            fallback_master=True, master_as_replica_weight=0,
        ) as connection:
            is_replica = await scalar(connection, "SELECT pg_is_in_recovery()")

    assert is_replica is False


@pytest.mark.parametrize("adapter,scalar,pool_kwargs", _SMOKE_ADAPTERS)
@pytest.mark.parametrize(
    "checker_factory,checker_kwargs",
    [
        pytest.param(BytesStalenessChecker, {"max_lag_bytes": 0}, id="bytes"),
        pytest.param(
            TimeStalenessChecker, {"max_lag": timedelta(seconds=1)}, id="time",
        ),
    ],
)
async def test_real_pg_staleness_checker_accepts_native_master_lsn(
    adapter, scalar, pool_kwargs, pg_dsn, checker_factory, checker_kwargs,
):
    checker = checker_factory(**checker_kwargs)
    async with _smoke_manager(adapter, pg_dsn, pool_kwargs) as manager:
        async with manager.acquire_master() as connection:
            ctx = CheckContext(connection, manager._pool_state.driver)
            await checker.collect_master_state(ctx)
            # The primary has no replay position. Both checkers must report
            # unknown replay state as stale, not crash on native LSN decoding.
            result = await checker.check(ctx)

    assert result == StalenessCheckResult(is_stale=True, lag={})
