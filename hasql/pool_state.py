import asyncio
import logging
from collections import defaultdict
from collections.abc import Sequence
from itertools import chain
from types import MappingProxyType
from typing import (
    Generic,
    Protocol,
    TypeVar,
    runtime_checkable,
)

from .abc import PoolDriver
from .acquire import AcquireContext
from .metrics import PoolStats
from .utils import Dsn, Stopwatch

logger = logging.getLogger(__name__)

PoolT = TypeVar("PoolT")
ConnT = TypeVar("ConnT")


@runtime_checkable
class PoolStateProvider(Protocol[PoolT]):
    @property
    def master_pool_count(self) -> int: ...
    async def get_master_pools(self) -> list[PoolT]: ...
    async def get_replica_pools(
        self, fallback_master: bool = False,
    ) -> list[PoolT]: ...
    def get_pool_freesize(self, pool: PoolT) -> int: ...
    def get_last_response_time(self, pool: PoolT) -> float | None: ...


class PoolState(Generic[PoolT, ConnT]):
    """Owns the driver and all pool state: master/replica sets,
    pool lifecycle, connection operations, waiting, and readiness."""

    _dsn_ready_event: defaultdict[Dsn, asyncio.Event]
    _dsn_check_cond: defaultdict[Dsn, asyncio.Condition]
    _master_pool_set: set[PoolT]
    _replica_pool_set: set[PoolT]

    def __init__(
        self,
        dsn_list: list[Dsn],
        driver: PoolDriver[PoolT, ConnT],
        stopwatch_window_size: int,
        pool_factory_kwargs: dict | None = None,
    ):
        self._driver = driver
        self._pool_factory_kwargs: MappingProxyType = MappingProxyType(
            self._driver.prepare_pool_factory_kwargs(
                dict(pool_factory_kwargs)
                if pool_factory_kwargs is not None
                else {},
            ),
        )
        self._dsn = list(dsn_list)
        self._pools: list[PoolT | None] = [None] * len(dsn_list)
        self._dsn_ready_event = defaultdict(asyncio.Event)
        self._dsn_check_cond = defaultdict(asyncio.Condition)
        self._master_pool_set: set[PoolT] = set()
        self._replica_pool_set: set[PoolT] = set()
        self._master_cond = asyncio.Condition()
        self._replica_cond = asyncio.Condition()
        self._stopwatch: Stopwatch[PoolT] = Stopwatch(
            window_size=stopwatch_window_size,
        )

    # --- Properties ---

    @property
    def driver(self) -> PoolDriver[PoolT, ConnT]:
        return self._driver

    @property
    def dsn(self) -> Sequence[Dsn]:
        return tuple(self._dsn)

    @property
    def pools(self) -> Sequence[PoolT | None]:
        return tuple(self._pools)

    @property
    def pool_factory_kwargs(self) -> MappingProxyType:
        return self._pool_factory_kwargs

    @property
    def master_pool_count(self) -> int:
        return len(self._master_pool_set)

    @property
    def replica_pool_count(self) -> int:
        return len(self._replica_pool_set)

    @property
    def available_pool_count(self) -> int:
        return self.master_pool_count + self.replica_pool_count

    # --- Pool state queries ---

    def pool_is_master(self, pool: PoolT) -> bool:
        return pool in self._master_pool_set

    def pool_is_replica(self, pool: PoolT) -> bool:
        return pool in self._replica_pool_set

    def get_pool_freesize(self, pool: PoolT) -> int:
        return self._driver.get_pool_freesize(pool)

    def get_last_response_time(self, pool: PoolT) -> float | None:
        return self._stopwatch.get_time(pool)

    # --- Driver operations ---

    def acquire_from_pool(
        self,
        pool: PoolT,
        *,
        timeout: float | None = None,
        **kwargs,
    ) -> AcquireContext[ConnT]:
        return self._driver.acquire_from_pool(pool, timeout=timeout, **kwargs)

    async def release_to_pool(
        self,
        connection: ConnT,
        pool: PoolT,
        **kwargs,
    ):
        await self._driver.release_to_pool(connection, pool, **kwargs)

    async def pool_factory(self, dsn: Dsn) -> PoolT:
        return await self._driver.pool_factory(dsn, **self._pool_factory_kwargs)

    async def close_pool(self, pool: PoolT):
        await self._driver.close_pool(pool)

    async def terminate_pool(self, pool: PoolT):
        await self._driver.terminate_pool(pool)

    def is_connection_closed(self, connection: ConnT) -> bool:
        return self._driver.is_connection_closed(connection)

    def host(self, pool: PoolT) -> str:
        return self._driver.host(pool)

    def pool_stats(self, pool: PoolT) -> PoolStats:
        return self._driver.pool_stats(pool)

    # --- Pool retrieval (async, waits for availability) ---

    async def get_master_pools(self) -> list[PoolT]:
        await self.wait_for_master_pools()
        return list(self._master_pool_set)

    async def get_replica_pools(
        self,
        fallback_master: bool = False,
    ) -> list[PoolT]:
        if not self._replica_pool_set and fallback_master:
            return await self.get_master_pools()
        await self.wait_for_replica_pools()
        return list(self._replica_pool_set)

    # --- Pool waiting ---

    async def wait_for_master_pools(self) -> None:
        async with self._master_cond:
            await self._master_cond.wait_for(
                lambda: bool(self._master_pool_set),
            )

    async def wait_for_replica_pools(
        self,
        fallback_master: bool = False,
    ) -> None:
        if not self._replica_pool_set and fallback_master:
            await self.wait_for_master_pools()
            return
        async with self._replica_cond:
            await self._replica_cond.wait_for(
                lambda: bool(self._replica_pool_set),
            )

    async def wait_masters_ready(self, masters_count: int):
        def predicate():
            return self.master_pool_count >= masters_count

        async with self._master_cond:
            await self._master_cond.wait_for(predicate)

    async def wait_replicas_ready(self, replicas_count: int):
        def predicate():
            return self.replica_pool_count >= replicas_count

        async with self._replica_cond:
            await self._replica_cond.wait_for(predicate)

    async def wait_all_ready(self):
        for dsn in self._dsn:
            await self._dsn_ready_event[dsn].wait()

    async def ready(
        self,
        masters_count: int | None = None,
        replicas_count: int | None = None,
        timeout: int = 10,
    ):
        if (masters_count is not None and replicas_count is None) or (
            masters_count is None and replicas_count is not None
        ):
            raise ValueError(
                "Arguments master_count and replicas_count "
                "should both be either None or not None",
            )

        if masters_count is not None and masters_count < 0:
            raise ValueError("masters_count shouldn't be negative")
        if replicas_count is not None and replicas_count < 0:
            raise ValueError("replicas_count shouldn't be negative")

        if masters_count is None or replicas_count is None:
            await asyncio.wait_for(self.wait_all_ready(), timeout=timeout)
            return

        await asyncio.wait_for(
            asyncio.gather(
                self.wait_masters_ready(masters_count),
                self.wait_replicas_ready(replicas_count),
            ),
            timeout=timeout,
        )

    async def wait_next_pool_check(self, timeout: int = 10):
        tasks = [self._wait_checking_pool(dsn) for dsn in self._dsn]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)

    async def _wait_checking_pool(self, dsn: Dsn):
        async with self._dsn_check_cond[dsn]:
            for _ in range(2):
                await self._dsn_check_cond[dsn].wait()

    # --- Pool role refresh ---

    async def refresh_pool_role(
        self, pool: PoolT, dsn: Dsn, sys_connection: ConnT,
    ):
        with self._stopwatch(pool):
            is_master = await self._driver.is_master(sys_connection)
        if is_master:
            await self._add_pool_to_master_set(pool, dsn)
            self._remove_pool_from_replica_set(pool, dsn)
        else:
            await self._add_pool_to_replica_set(pool, dsn)
            self._remove_pool_from_master_set(pool, dsn)
        self._dsn_ready_event[dsn].set()

    def remove_pool_from_all_sets(self, pool: PoolT, dsn: Dsn):
        self._remove_pool_from_master_set(pool, dsn)
        self._remove_pool_from_replica_set(pool, dsn)

    def clear_sets(self):
        self._master_pool_set.clear()
        self._replica_pool_set.clear()

    # --- Pool registry ---

    def set_pool(self, index: int, pool: PoolT):
        self._pools[index] = pool

    async def notify_pool_checked(self, dsn: Dsn):
        async with self._dsn_check_cond[dsn]:
            self._dsn_check_cond[dsn].notify_all()

    # --- Pool state mutations ---

    async def _add_pool_to_master_set(self, pool: PoolT, dsn: Dsn):
        if pool in self._master_pool_set:
            return
        self._master_pool_set.add(pool)
        logger.debug(
            "Pool %s has been added to master set",
            dsn.with_(password="******"),
        )
        async with self._master_cond:
            self._master_cond.notify_all()

    async def _add_pool_to_replica_set(self, pool: PoolT, dsn: Dsn):
        if pool in self._replica_pool_set:
            return
        self._replica_pool_set.add(pool)
        logger.debug(
            "Pool %s has been added to replica set",
            dsn.with_(password="******"),
        )
        async with self._replica_cond:
            self._replica_cond.notify_all()

    def _remove_pool_from_master_set(self, pool: PoolT, dsn: Dsn):
        if pool in self._master_pool_set:
            self._master_pool_set.remove(pool)
            logger.debug(
                "Pool %s has been removed from master set",
                dsn.with_(password="******"),
            )

    def _remove_pool_from_replica_set(self, pool: PoolT, dsn: Dsn):
        if pool in self._replica_pool_set:
            self._replica_pool_set.remove(pool)
            logger.debug(
                "Pool %s has been removed from replica set",
                dsn.with_(password="******"),
            )

    # --- Iteration ---

    def __iter__(self):
        return chain(iter(self._master_pool_set), iter(self._replica_pool_set))


__all__ = ("PoolState", "PoolStateProvider")
