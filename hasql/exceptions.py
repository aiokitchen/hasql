class HasqlError(Exception):
    """Base exception for all hasql errors."""


class PoolManagerClosedError(HasqlError):
    """Raised when acquire() is called on a closed or closing pool manager."""


class PoolManagerClosingError(HasqlError):
    """Raised when pool creation is exhausted during manager shutdown."""


class NoAvailablePoolError(HasqlError):
    """Raised when the balancer cannot find a suitable pool for the request."""


class UnexpectedDatabaseResponseError(HasqlError):
    """Raised when a database query returns an unexpected result."""


__all__ = (
    "HasqlError",
    "NoAvailablePoolError",
    "PoolManagerClosedError",
    "PoolManagerClosingError",
    "UnexpectedDatabaseResponseError",
)
