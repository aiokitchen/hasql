import asyncio
import logging
from collections import defaultdict
from itertools import chain
from typing import (
    DefaultDict,
    Generic,
    List,
    Optional,
    Protocol,
    Sequence,
    Set,
    TypeVar,
    runtime_checkable,
)

from .abc import PoolDriver
from .utils import Dsn, Stopwatch

logger = logging.getLogger(__name__)

PoolT = TypeVar("PoolT")
ConnT = TypeVar("ConnT")


@runtime_checkable
class PoolStateProvider(Protocol[PoolT]):
    @property
    def master_pool_count(self) -> int: ...
    async def get_master_pools(self) -> List[PoolT]: ...
    async def get_replica_pools(
        self, fallback_master: bool = False,
    ) -> List[PoolT]: ...
    def get_pool_freesize(self, pool: PoolT) -> int: ...
    def get_last_response_time(self, pool: PoolT) -> Optional[float]: ...


class PoolState(Generic[PoolT, ConnT]):
    """Manages pool state: master/replica sets, waiting, readiness,
    and pool queries."""

    _dsn_ready_event: DefaultDict[Dsn, asyncio.Event]
    _dsn_check_cond: DefaultDict[Dsn, asyncio.Condition]
    _master_pool_set: Set[PoolT]
    _replica_pool_set: Set[PoolT]

    def __init__(
        self,
        dsn_list: List[Dsn],
        driver: PoolDriver[PoolT, ConnT],
        stopwatch_window_size: int,
    ):
        self._driver = driver
        self._dsn = dsn_list
        self._pools: List[Optional[PoolT]] = [None] * len(dsn_list)
        self._dsn_ready_event = defaultdict(asyncio.Event)
        self._dsn_check_cond = defaultdict(asyncio.Condition)
        self._master_pool_set: Set[PoolT] = set()
        self._replica_pool_set: Set[PoolT] = set()
        self._master_cond = asyncio.Condition()
        self._replica_cond = asyncio.Condition()
        self._stopwatch: Stopwatch[PoolT] = Stopwatch(
            window_size=stopwatch_window_size,
        )

    # --- Properties ---

    @property
    def dsn(self) -> List[Dsn]:
        return self._dsn

    @property
    def pools(self) -> Sequence[Optional[PoolT]]:
        return tuple(self._pools)

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

    def get_last_response_time(self, pool: PoolT) -> Optional[float]:
        return self._stopwatch.get_time(pool)

    # --- Pool retrieval (async, waits for availability) ---

    async def get_master_pools(self) -> List[PoolT]:
        await self.wait_for_master_pools()
        return list(self._master_pool_set)

    async def get_replica_pools(
        self,
        fallback_master: bool = False,
    ) -> List[PoolT]:
        if not self._replica_pool_set and fallback_master:
            return await self.get_master_pools()
        await self.wait_for_replica_pools()
        return list(self._replica_pool_set)

    # --- Pool waiting ---

    async def wait_for_master_pools(self) -> None:
        if not self._master_pool_set:
            async with self._master_cond:
                await self._master_cond.wait()

    async def wait_for_replica_pools(
        self,
        fallback_master: bool = False,
    ) -> None:
        if not self._replica_pool_set:
            if fallback_master:
                await self.wait_for_master_pools()
                return
            async with self._replica_cond:
                await self._replica_cond.wait()

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
        masters_count: Optional[int] = None,
        replicas_count: Optional[int] = None,
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

        if masters_count is None and replicas_count is None:
            await asyncio.wait_for(self.wait_all_ready(), timeout=timeout)
            return

        if masters_count is None or replicas_count is None:
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

    # --- Pool state mutations (used by health monitor via BasePoolManager) ---

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
