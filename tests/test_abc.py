"""Tests for PoolDriver ABC."""

import pytest

from hasql.abc import PoolDriver
from hasql.metrics import PoolStats


class CompleteDriver(PoolDriver):
    def get_pool_freesize(self, pool):
        return 0

    def acquire_from_pool(self, pool, *, timeout=None, **kwargs):
        return None

    async def release_to_pool(self, connection, pool, **kwargs):
        pass

    async def is_master(self, connection):
        return True

    async def fetch_scalar(self, connection, query):
        return None

    async def pool_factory(self, dsn, **kwargs):
        return object()

    async def close_pool(self, pool):
        pass

    async def terminate_pool(self, pool):
        pass

    def is_connection_closed(self, connection):
        return False

    def host(self, pool):
        return "localhost"

    def pool_stats(self, pool):
        return PoolStats(min=0, max=1, idle=1, used=0)


def test_pool_driver_cannot_be_instantiated():
    with pytest.raises(TypeError):
        PoolDriver()


def test_pool_driver_requires_all_abstract_methods():
    class IncompleteDriver(PoolDriver):
        pass

    with pytest.raises(TypeError):
        IncompleteDriver()


def test_pool_driver_prepare_pool_factory_kwargs_default():
    driver = CompleteDriver()
    kwargs = {"minsize": 5, "maxsize": 20}

    result = driver.prepare_pool_factory_kwargs(kwargs)

    assert (result is kwargs, result) == (
        True,
        {"minsize": 5, "maxsize": 20},
    )


def test_complete_driver_is_pool_driver():
    driver = CompleteDriver()

    assert isinstance(driver, PoolDriver)
