import asyncio
import os
from contextlib import asynccontextmanager

import aiomisc
import pytest


@pytest.fixture(autouse=True)
def aiomisc_test_timeout():
    return 5


class UnavailableDbServer(aiomisc.service.TCPServer):
    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ):
        while await reader.read(65534):
            pass
        writer.close()
        await writer.wait_closed()


@pytest.fixture
def db_server_port(aiomisc_unused_port_factory) -> int:
    return aiomisc_unused_port_factory()


@pytest.fixture
def services(db_server_port, localhost):
    return []  # [UnavailableDbServer(port=db_server_port, address=localhost)]


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    return os.environ.get(
        "PG_DSN",
        "postgres://test:test@localhost:5432/test",
    )


@asynccontextmanager
async def setup_aiopg(pg_dsn):
    from hasql.aiopg import PoolManager
    pool = PoolManager(dsn=pg_dsn, fallback_master=True)
    yield pool
    await pool.close()


@asynccontextmanager
async def setup_aiopgsa(pg_dsn):
    from hasql.aiopg_sa import PoolManager
    pool = PoolManager(dsn=pg_dsn, fallback_master=True)
    yield pool
    await pool.close()


@asynccontextmanager
async def setup_asyncpg(pg_dsn):
    from hasql.asyncpg import PoolManager
    pool = PoolManager(dsn=pg_dsn, fallback_master=True)
    yield pool
    await pool.close()


@asynccontextmanager
async def setup_asyncsqlalchemy(pg_dsn):
    from hasql.asyncsqlalchemy import PoolManager
    pool = PoolManager(dsn=pg_dsn, fallback_master=True)
    yield pool
    await pool.close()


@asynccontextmanager
async def setup_psycopg3(pg_dsn):
    from hasql.psycopg3 import PoolManager
    pool = PoolManager(dsn=pg_dsn, fallback_master=True)
    yield pool
    await pool.close()
