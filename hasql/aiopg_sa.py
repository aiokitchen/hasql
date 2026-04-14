# Backward-compatible re-export shim.
# The driver has moved to hasql.driver.aiopg_sa.
from hasql.driver.aiopg_sa import AiopgSaDriver, PoolManager  # noqa: F401

__all__ = ("AiopgSaDriver", "PoolManager")
