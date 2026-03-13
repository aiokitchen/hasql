import asyncio
import logging
from typing import TYPE_CHECKING, Generic, List, Optional, TypeVar

from .utils import Dsn

if TYPE_CHECKING:
    from .pool_manager import BasePoolManager

logger = logging.getLogger(__name__)

PoolT = TypeVar("PoolT")
ConnT = TypeVar("ConnT")


class PoolHealthMonitor(Generic[PoolT, ConnT]):
    """Background health monitor that checks pool roles periodically."""

    def __init__(self, manager: "BasePoolManager[PoolT, ConnT]"):
        self._manager = manager
        self._tasks: Optional[List[asyncio.Task]] = [
            asyncio.create_task(self._check_pool_task(index))
            for index in range(len(manager.pool_state._dsn))
        ]

    @property
    def tasks(self) -> Optional[List[asyncio.Task]]:
        return self._tasks

    async def stop(self):
        if self._tasks is not None:
            for task in self._tasks:
                task.cancel()

            await asyncio.gather(
                *self._tasks,
                return_exceptions=True,
            )

            self._tasks = None

    async def _check_pool_task(self, index: int):
        logger.debug("Starting pool task")
        manager = self._manager
        pool_state = manager.pool_state
        dsn = pool_state._dsn[index]
        censored_dsn = str(dsn.with_(password="******"))
        pool = await self._wait_creating_pool(dsn)
        pool_state._pools[index] = pool

        logger.debug("Setting dsn=%r event", censored_dsn)
        sys_connection: Optional[ConnT] = None
        while not manager.closing:
            try:
                # Don't use async with — we need a custom timeout
                logger.debug(
                    "Acquiring connection for checking dsn=%r",
                    censored_dsn,
                )
                sys_connection = await asyncio.wait_for(
                    manager.acquire_from_pool(pool),
                    timeout=manager.refresh_timeout,
                )

                logger.debug("Checking dsn=%r", censored_dsn)
                if sys_connection is None:
                    continue
                await manager._periodic_pool_check(
                    pool,
                    dsn,
                    sys_connection,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Creating system connection failed for dsn=%r",
                    censored_dsn,
                )
                pool_state._remove_pool_from_master_set(pool, dsn)
                pool_state._remove_pool_from_replica_set(pool, dsn)
            except asyncio.CancelledError as cancelled_error:
                if manager.closing:
                    raise cancelled_error from None
                logger.warning(
                    "Cancelled error for dsn=%r",
                    censored_dsn,
                    exc_info=True,
                )
                pool_state._remove_pool_from_master_set(pool, dsn)
                pool_state._remove_pool_from_replica_set(pool, dsn)
            except Exception:
                logger.warning(
                    "Database is not available with exception for dsn=%r",
                    censored_dsn,
                    exc_info=True,
                )
                pool_state._remove_pool_from_master_set(pool, dsn)
                pool_state._remove_pool_from_replica_set(pool, dsn)
            finally:
                if sys_connection is not None:
                    await self._safe_release_connection(
                        sys_connection, pool, censored_dsn,
                    )
                    sys_connection = None
                await self._notify_about_pool_has_checked(dsn)

            await asyncio.sleep(manager.refresh_delay)

    async def _safe_release_connection(
        self, connection: ConnT, pool: PoolT, censored_dsn: str,
    ):
        manager = self._manager
        try:
            await manager.release_to_pool(connection, pool)
        except asyncio.CancelledError as cancelled_error:
            if manager.closing:
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
        manager = self._manager
        while not manager.closing:
            try:
                return await asyncio.wait_for(
                    manager._pool_factory(dsn),
                    timeout=manager.refresh_timeout,
                )
            except Exception:
                logger.warning(
                    "Creating pool failed with exception for dsn=%s",
                    dsn.with_(password="******"),
                    exc_info=True,
                )
                await asyncio.sleep(manager.refresh_delay)
        raise asyncio.CancelledError("Pool manager is closing")

    async def _notify_about_pool_has_checked(self, dsn: Dsn):
        pool_state = self._manager.pool_state
        async with pool_state._dsn_check_cond[dsn]:
            pool_state._dsn_check_cond[dsn].notify_all()


__all__ = ("PoolHealthMonitor",)
