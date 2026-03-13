import pytest

from hasql.balancer_policy import AbstractBalancerPolicy


def test_abstract_balancer_policy_cannot_instantiate():
    with pytest.raises(TypeError, match="abstract"):
        AbstractBalancerPolicy(None)


def test_abstract_balancer_policy_is_generic():
    # Verify parameterization works
    parameterized = AbstractBalancerPolicy[int]
    assert parameterized is not None
