import asyncio
import logging
from collections.abc import Callable
from typing import Generic, TypeVar

from .exceptions import PoolManagerClosingError
from .pool_state import PoolState
from .utils import Dsn

logger = logging.getLogger(__name__)

PoolT = TypeVar("PoolT")
ConnT = TypeVar("ConnT")


class PoolHealthMonitor(Generic[PoolT, ConnT]):
    """Background health monitor that checks pool roles periodically."""

    def __init__(
        self,
        pool_state: PoolState[PoolT, ConnT],
        refresh_delay: float,
        refresh_timeout: float,
        closing_getter: Callable[[], bool],
    ):
        self._pool_state = pool_state
        self._refresh_delay = refresh_delay
        self._refresh_timeout = refresh_timeout
        self._closing = closing_getter
        self._tasks: list[asyncio.Task] | None = [
            asyncio.create_task(self._check_pool_task(index))
            for index in range(len(pool_state.dsn))
        ]

    @property
    def tasks(self) -> list[asyncio.Task] | None:
        return self._tasks

    async def stop(self):
        if self._tasks is not None:
            for task in self._tasks:
                task.cancel()

            # Tasks may finish with CancelledError or
            # PoolManagerClosingError — both are expected.
            await asyncio.gather(
                *self._tasks,
                return_exceptions=True,
            )

            self._tasks = None

    async def _periodic_pool_check(
        self,
        pool: PoolT,
        dsn: Dsn,
        sys_connection: ConnT,
    ):
        while not self._closing():
            try:
                await asyncio.wait_for(
                    self._pool_state.refresh_pool_role(
                        pool, dsn, sys_connection,
                    ),
                    timeout=self._refresh_timeout,
                )
                await self._pool_state.notify_pool_checked(dsn)
            except asyncio.TimeoutError:
                logger.warning(
                    "Periodic pool check failed for dsn=%s",
                    dsn.with_(password="******"),
                )
                self._pool_state.remove_pool_from_all_sets(pool, dsn)
                await self._pool_state.notify_pool_checked(dsn)

            await asyncio.sleep(self._refresh_delay)

    async def _check_pool_task(self, index: int):
        logger.debug("Starting pool task")
        pool_state = self._pool_state
        dsn = pool_state.dsn[index]
        censored_dsn = str(dsn.with_(password="******"))
        pool = await self._wait_creating_pool(dsn)
        pool_state.set_pool(index, pool)

        logger.debug("Setting dsn=%r event", censored_dsn)
        sys_connection: ConnT | None = None
        while not self._closing():
            try:
                # Don't use async with — we need a custom timeout
                logger.debug(
                    "Acquiring connection for checking dsn=%r",
                    censored_dsn,
                )
                sys_connection = await asyncio.wait_for(
                    pool_state.acquire_from_pool(pool),
                    timeout=self._refresh_timeout,
                )

                logger.debug("Checking dsn=%r", censored_dsn)
                if sys_connection is None:
                    continue
                await self._periodic_pool_check(pool, dsn, sys_connection)
            except asyncio.TimeoutError:
                logger.warning(
                    "Creating system connection failed for dsn=%r",
                    censored_dsn,
                )
                pool_state.remove_pool_from_all_sets(pool, dsn)
            except asyncio.CancelledError as cancelled_error:
                if self._closing():
                    raise cancelled_error from None
                logger.warning(
                    "Cancelled error for dsn=%r",
                    censored_dsn,
                    exc_info=True,
                )
                pool_state.remove_pool_from_all_sets(pool, dsn)
            except Exception:
                logger.warning(
                    "Database is not available with exception for dsn=%r",
                    censored_dsn,
                    exc_info=True,
                )
                pool_state.remove_pool_from_all_sets(pool, dsn)
            finally:
                if sys_connection is not None:
                    await self._safe_release_connection(
                        sys_connection, pool, censored_dsn,
                    )
                    sys_connection = None
                await pool_state.notify_pool_checked(dsn)

            await asyncio.sleep(self._refresh_delay)

    async def _safe_release_connection(
        self, connection: ConnT, pool: PoolT, censored_dsn: str,
    ):
        try:
            await self._pool_state.release_to_pool(connection, pool)
        except asyncio.CancelledError as cancelled_error:
            if self._closing():
                raise cancelled_error from None
            logger.warning(
                "Release connection to pool with "
                "Cancelled error for dsn=%r",
                censored_dsn,
                exc_info=True,
            )
        except Exception:
            logger.warning(
                "Release connection to pool with "
                "exception for dsn=%r",
                censored_dsn,
                exc_info=True,
            )

    async def _wait_creating_pool(self, dsn: Dsn) -> PoolT:
        pool_state = self._pool_state
        while not self._closing():
            try:
                return await asyncio.wait_for(
                    pool_state.pool_factory(dsn),
                    timeout=self._refresh_timeout,
                )
            except Exception:
                logger.warning(
                    "Creating pool failed with exception for dsn=%s",
                    dsn.with_(password="******"),
                    exc_info=True,
                )
                await asyncio.sleep(self._refresh_delay)
        raise PoolManagerClosingError("Pool manager is closing")


__all__ = ("PoolHealthMonitor",)
