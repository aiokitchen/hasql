from __future__ import annotations

import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

_WAL_LSN_PATTERN = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")


@dataclass(frozen=True, slots=True)
class StalenessCheckResult:
    is_stale: bool
    lag: dict[str, Any]


class CheckContext:
    """Pre-bound query context for staleness checks.
    Created by PoolState with the current connection and driver.
    """
    __slots__ = ("_connection", "_driver")

    def __init__(self, connection: Any, driver: Any) -> None:
        self._connection = connection
        self._driver = driver

    async def fetch_scalar(self, query: str) -> Any:
        return await self._driver.fetch_scalar(self._connection, query)


class BaseStalenessChecker(ABC):
    async def collect_master_state(self, ctx: CheckContext) -> None:
        pass

    @abstractmethod
    async def check(self, ctx: CheckContext) -> StalenessCheckResult: ...


class BytesStalenessChecker(BaseStalenessChecker):
    def __init__(
        self,
        max_lag_bytes: int,
        max_master_lsn_age: timedelta = timedelta(seconds=2),
    ) -> None:
        self._max_lag_bytes = max_lag_bytes
        self._max_master_lsn_age = max_master_lsn_age
        self._master_lsn: str | None = None
        self._master_lsn_updated_at: float | None = None

    async def collect_master_state(self, ctx: CheckContext) -> None:
        self._master_lsn = await ctx.fetch_scalar(
            "SELECT pg_current_wal_lsn()",
        )
        self._master_lsn_updated_at = time.monotonic()

    async def check(self, ctx: CheckContext) -> StalenessCheckResult:
        if self._master_lsn is None or self._master_lsn_updated_at is None:
            return StalenessCheckResult(is_stale=False, lag={})

        if not _WAL_LSN_PATTERN.match(self._master_lsn):
            return StalenessCheckResult(is_stale=True, lag={})

        age = time.monotonic() - self._master_lsn_updated_at
        if age > self._max_master_lsn_age.total_seconds():
            return StalenessCheckResult(is_stale=False, lag={})

        lag_bytes = await ctx.fetch_scalar(
            f"SELECT pg_wal_lsn_diff('{self._master_lsn}'::pg_lsn,"
            f" pg_last_wal_replay_lsn())::bigint",
        )
        if lag_bytes is None:
            return StalenessCheckResult(is_stale=True, lag={})
        return StalenessCheckResult(
            is_stale=lag_bytes > self._max_lag_bytes,
            lag={"bytes": lag_bytes},
        )


class TimeStalenessChecker(BaseStalenessChecker):
    def __init__(self, max_lag: timedelta) -> None:
        self._max_lag = max_lag

    async def check(self, ctx: CheckContext) -> StalenessCheckResult:
        lag = await ctx.fetch_scalar(
            "SELECT clock_timestamp() - pg_last_xact_replay_timestamp()",
        )
        if lag is None:
            return StalenessCheckResult(is_stale=True, lag={})
        return StalenessCheckResult(
            is_stale=lag > self._max_lag,
            lag={"time": lag},
        )


class StalenessPolicy:
    """User-facing configuration for staleness detection.
    Non-generic — pool identity is opaque (used only as dict keys).
    """

    def __init__(
        self,
        checker: BaseStalenessChecker,
        grace_period: timedelta | None = None,
    ) -> None:
        self._checker = checker
        self._grace_period = grace_period
        self._last_fresh_at: dict[Any, float] = {}

    async def check(
        self,
        pool: Any,
        ctx: CheckContext,
    ) -> StalenessCheckResult:
        result = await self._checker.check(ctx)
        if not result.is_stale:
            self._last_fresh_at[pool] = time.monotonic()
            return result
        if self._grace_period is not None and pool in self._last_fresh_at:
            age = time.monotonic() - self._last_fresh_at[pool]
            if age < self._grace_period.total_seconds():
                return StalenessCheckResult(is_stale=False, lag=result.lag)
        return result

    async def collect_master_state(self, ctx: CheckContext) -> None:
        await self._checker.collect_master_state(ctx)

    def remove_pool(self, pool: Any) -> None:
        self._last_fresh_at.pop(pool, None)


__all__ = (
    "StalenessCheckResult",
    "CheckContext",
    "BaseStalenessChecker",
    "TimeStalenessChecker",
    "BytesStalenessChecker",
    "StalenessPolicy",
)
