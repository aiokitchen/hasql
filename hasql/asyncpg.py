# Backward-compatible re-export shim.
# The driver has moved to hasql.driver.asyncpg.
from hasql.driver.asyncpg import AsyncpgDriver, PoolManager  # noqa: F401

__all__ = ("AsyncpgDriver", "PoolManager")
