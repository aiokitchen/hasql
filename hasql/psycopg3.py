# Backward-compatible re-export shim.
# The driver has moved to hasql.driver.psycopg3.
from hasql.driver.psycopg3 import (  # noqa: F401
    PoolManager,
    Psycopg3AcquireContext,
    Psycopg3Driver,
)

__all__ = ("Psycopg3Driver", "Psycopg3AcquireContext", "PoolManager")
