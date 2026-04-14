# Backward-compatible re-export shim.
# The driver has moved to hasql.driver.asyncpgsa.
from hasql.driver.asyncpgsa import AsyncpgsaDriver, PoolManager  # noqa: F401

__all__ = ("AsyncpgsaDriver", "PoolManager")
