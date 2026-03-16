"""Tests for PoolDriver ABC."""
import pytest

from hasql.abc import PoolDriver


def test_pool_driver_cannot_be_instantiated():
    with pytest.raises(TypeError):
        PoolDriver()


def test_pool_driver_requires_all_abstract_methods():
    class IncompleteDriver(PoolDriver):
        pass

    with pytest.raises(TypeError):
        IncompleteDriver()


def test_pool_driver_prepare_pool_factory_kwargs_default():
    """Default prepare_pool_factory_kwargs returns kwargs unchanged."""
    from tests.mocks.pool_manager import TestDriver

    driver = TestDriver()
    kwargs = {"minsize": 5, "maxsize": 20}
    result = driver.prepare_pool_factory_kwargs(kwargs)
    assert result is kwargs
    assert result == {"minsize": 5, "maxsize": 20}


def test_test_driver_is_pool_driver():
    from tests.mocks.pool_manager import TestDriver

    driver = TestDriver()
    assert isinstance(driver, PoolDriver)


async def test_pool_manager_has_driver_via_pool_state():
    """BasePoolManager exposes driver via _pool_state."""
    from tests.mocks import TestPoolManager

    manager = TestPoolManager(
        "postgresql://test:test@master:5432/test",
    )
    try:
        assert isinstance(manager._pool_state.driver, PoolDriver)
    finally:
        await manager.close()
