# Backward-compatible re-export shim.
# The driver has moved to hasql.driver.aiopg.
from hasql.driver.aiopg import AiopgDriver, PoolManager  # noqa: F401

__all__ = ("AiopgDriver", "PoolManager")
