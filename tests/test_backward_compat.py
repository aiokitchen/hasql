"""Test that all old import paths still work after the split."""


def test_base_exports_pool_manager():
    from hasql.base import BasePoolManager
    from hasql.pool_manager import BasePoolManager as Direct

    assert BasePoolManager is Direct


def test_base_exports_abstract_balancer_policy():
    from hasql.balancer_policy import AbstractBalancerPolicy as Direct
    from hasql.base import AbstractBalancerPolicy

    assert AbstractBalancerPolicy is Direct


def test_base_exports_timeout_acquire_context():
    from hasql.acquire import TimeoutAcquireContext as Direct
    from hasql.base import TimeoutAcquireContext

    assert TimeoutAcquireContext is Direct


def test_base_exports_pool_acquire_context():
    from hasql.acquire import PoolAcquireContext as Direct
    from hasql.base import PoolAcquireContext

    assert PoolAcquireContext is Direct


def test_base_exports_acquire_context():
    from hasql.acquire import AcquireContext as Direct
    from hasql.base import AcquireContext

    assert AcquireContext is Direct


def test_base_exports_type_vars():
    from hasql.base import ConnT, PoolT
    from hasql.pool_manager import ConnT as DirectConnT
    from hasql.pool_manager import PoolT as DirectPoolT

    assert PoolT is DirectPoolT
    assert ConnT is DirectConnT


def test_base_exports_pool_driver():
    from hasql.abc import PoolDriver as Direct
    from hasql.base import PoolDriver

    assert PoolDriver is Direct


def test_base_exports_pool_state():
    from hasql.base import PoolState
    from hasql.pool_state import PoolState as Direct

    assert PoolState is Direct


def test_base_exports_pool_state_provider():
    from hasql.base import PoolStateProvider
    from hasql.pool_state import PoolStateProvider as Direct

    assert PoolStateProvider is Direct
