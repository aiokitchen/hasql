import asyncio
import os

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
    return[]   # [UnavailableDbServer(port=db_server_port, address=localhost)]


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    return os.environ.get(
        "PG_DSN",
        "postgres://test:test@localhost:5432/test",
    )
