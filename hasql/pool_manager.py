import asyncio
import logging
from collections import defaultdict
from itertools import chain
from types import MappingProxyType
from typing import (
    DefaultDict,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Set,
    TypeVar,
    Union,
)

from .abc import PoolDriver
from .acquire import AcquireContext, PoolAcquireContext
from .balancer_policy.base import AbstractBalancerPolicy
from .constants import (
    DEFAULT_ACQUIRE_TIMEOUT,
    DEFAULT_MASTER_AS_REPLICA_WEIGHT,
    DEFAULT_REFRESH_DELAY,
    DEFAULT_REFRESH_TIMEOUT,
    DEFAULT_STOPWATCH_WINDOW_SIZE,
)
from .health import PoolHealthMonitor
from .metrics import CalculateMetrics, DriverMetrics, Metrics
from .utils import Dsn, Stopwatch, split_dsn

logger = logging.getLogger(__name__)

PoolT = TypeVar("PoolT")
ConnT = TypeVar("ConnT")


class BasePoolManager(Generic[PoolT, ConnT]):
    _dsn_ready_event: DefaultDict[Dsn, asyncio.Event]
    _dsn_check_cond: DefaultDict[Dsn, asyncio.Condition]
    _master_pool_set: Set[PoolT]
    _replica_pool_set: Set[PoolT]
    _unmanaged_connections: Dict[ConnT, PoolT]

    def __init__(
        self,
        dsn: str,
        *,
        driver: PoolDriver[PoolT, ConnT],
        acquire_timeout: Union[float, int] = DEFAULT_ACQUIRE_TIMEOUT,
        refresh_delay: Union[float, int] = DEFAULT_REFRESH_DELAY,
        refresh_timeout: Union[float, int] = DEFAULT_REFRESH_TIMEOUT,
        fallback_master: bool = False,
        master_as_replica_weight: float = DEFAULT_MASTER_AS_REPLICA_WEIGHT,
        balancer_policy: type = AbstractBalancerPolicy,
        stopwatch_window_size: int = DEFAULT_STOPWATCH_WINDOW_SIZE,
        pool_factory_kwargs: Optional[dict] = None,
    ):
        if not issubclass(balancer_policy, AbstractBalancerPolicy):
            raise ValueError(
                "balancer_policy must be a subclass of AbstractBalancerPolicy",
            )

        if balancer_policy is AbstractBalancerPolicy:
            # Avoid circular import
            from .balancer_policy.greedy import GreedyBalancerPolicy

            balancer_policy = GreedyBalancerPolicy

        self._driver = driver

        if pool_factory_kwargs is None:
            pool_factory_kwargs = {}
        self._pool_factory_kwargs = MappingProxyType(
            self._driver.prepare_pool_factory_kwargs(pool_factory_kwargs),
        )
        self._dsn: List[Dsn] = split_dsn(dsn)
        self._dsn_ready_event = defaultdict(asyncio.Event)
        self._dsn_check_cond = defaultdict(asyncio.Condition)
        self._pools: List[Optional[PoolT]] = [None] * len(self._dsn)
        self._acquire_timeout = acquire_timeout
        self._refresh_delay = refresh_delay
        self._refresh_timeout = refresh_timeout
        self._fallback_master = fallback_master
        self._master_as_replica_weight = master_as_replica_weight
        self._balancer: Optional[AbstractBalancerPolicy[PoolT]] = (
            balancer_policy(self)
        )
        self._master_pool_set: Set[PoolT] = set()
        self._replica_pool_set: Set[PoolT] = set()
        self._master_cond = asyncio.Condition()
        self._replica_cond = asyncio.Condition()
        self._unmanaged_connections: Dict[ConnT, PoolT] = {}
        self._stopwatch: Stopwatch[PoolT] = Stopwatch(
            window_size=stopwatch_window_size,
        )
        self._health: PoolHealthMonitor[PoolT, ConnT] = PoolHealthMonitor(
            self,
        )
        self._closing = False
        self._closed = False
        self._metrics = CalculateMetrics()

    @property
    def driver(self) -> PoolDriver[PoolT, ConnT]:
        return self._driver

    @property
    def dsn(self) -> List[Dsn]:
        return self._dsn

    @property
    def refresh_delay(self):
        return self._refresh_delay

    @property
    def refresh_timeout(self):
        return self._refresh_timeout

    @property
    def pool_factory_kwargs(self):
        return self._pool_factory_kwargs

    @property
    def master_pool_count(self):
        return len(self._master_pool_set)

    @property
    def replica_pool_count(self):
        return len(self._replica_pool_set)

    @property
    def available_pool_count(self):
        return self.master_pool_count + self.replica_pool_count

    @property
    def balancer(self) -> Optional[AbstractBalancerPolicy[PoolT]]:
        return self._balancer

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def pools(self) -> Sequence[Optional[PoolT]]:
        return tuple(self._pools)

    # --- Driver proxy methods ---

    def get_pool_freesize(self, pool: PoolT) -> int:
        return self._driver.get_pool_freesize(pool)

    def acquire_from_pool(
        self,
        pool: PoolT,
        *,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> AcquireContext[ConnT]:
        return self._driver.acquire_from_pool(
            pool,
            timeout=timeout,
            **kwargs,
        )

    async def release_to_pool(
        self,
        connection: ConnT,
        pool: PoolT,
        **kwargs,
    ):
        await self._driver.release_to_pool(connection, pool, **kwargs)

    async def _is_master(self, connection: ConnT) -> bool:
        return await self._driver.is_master(connection)

    async def _pool_factory(self, dsn: Dsn) -> PoolT:
        return await self._driver.pool_factory(dsn, **self._pool_factory_kwargs)

    async def _close(self, pool: PoolT):
        await self._driver.close_pool(pool)

    async def _terminate(self, pool: PoolT) -> None:
        await self._driver.terminate_pool(pool)

    def is_connection_closed(self, connection: ConnT) -> bool:
        return self._driver.is_connection_closed(connection)

    def host(self, pool: PoolT) -> str:
        return self._driver.host(pool)

    def _driver_metrics(self) -> Sequence[DriverMetrics]:
        return self._driver.driver_metrics(self._pools)

    # --- End driver proxy methods ---

    def metrics(self) -> Metrics:
        return Metrics(
            drivers=self._driver_metrics(),
            hasql=self._metrics.metrics(),
        )

    def acquire(
        self,
        read_only: bool = False,
        fallback_master: Optional[bool] = None,
        master_as_replica_weight: Optional[float] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> "PoolAcquireContext[PoolT, ConnT]":
        if self._closed or self._closing:
            raise RuntimeError("Pool manager is closed")

        if fallback_master is None:
            fallback_master = self._fallback_master

        if not read_only and master_as_replica_weight is not None:
            raise ValueError(
                "Field master_as_replica_weight is used only when "
                "read_only is True",
            )
        if master_as_replica_weight is not None and not (
            0.0 <= master_as_replica_weight <= 1
        ):
            raise ValueError(
                "Field master_as_replica_weight must belong "
                "to the segment [0; 1]",
            )

        if read_only:
            if master_as_replica_weight is None:
                master_as_replica_weight = self._master_as_replica_weight

        if timeout is None:
            timeout = self._acquire_timeout

        ctx = PoolAcquireContext(
            pool_manager=self,
            read_only=read_only,
            fallback_master=fallback_master,
            master_as_replica_weight=master_as_replica_weight,
            timeout=timeout,
            metrics=self._metrics,
            **kwargs,
        )

        return ctx

    def acquire_master(
        self,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> "PoolAcquireContext[PoolT, ConnT]":
        return self.acquire(read_only=False, timeout=timeout, **kwargs)

    def acquire_replica(
        self,
        fallback_master: Optional[bool] = None,
        master_as_replica_weight: Optional[float] = None,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> "PoolAcquireContext[PoolT, ConnT]":
        return self.acquire(
            read_only=True,
            fallback_master=fallback_master,
            master_as_replica_weight=master_as_replica_weight,
            timeout=timeout,
            **kwargs,
        )

    async def release(self, connection: ConnT, **kwargs):
        if connection not in self._unmanaged_connections:
            raise ValueError(
                "Pool.release() received invalid connection: "
                f"{connection!r} is not a member of this pool",
            )

        pool = self._unmanaged_connections.pop(connection)
        self._metrics.remove_connection(self.host(pool))
        await self.release_to_pool(connection, pool, **kwargs)

    async def close(self):
        self._closing = True
        await self._clear()
        await asyncio.gather(
            *[self._close(pool) for pool in self._pools if pool is not None],
            return_exceptions=True,
        )
        self._closing = False
        self._closed = True

    async def terminate(self):
        self._closing = True
        await self._clear()
        for pool in self._pools:
            if pool is None:
                continue
            await self._terminate(pool)
        self._closing = False
        self._closed = True

    async def wait_next_pool_check(self, timeout: int = 10):
        tasks = [self._wait_checking_pool(dsn) for dsn in self._dsn]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)

    async def _wait_checking_pool(self, dsn: Dsn):
        async with self._dsn_check_cond[dsn]:
            for _ in range(2):
                await self._dsn_check_cond[dsn].wait()

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

    async def wait_all_ready(self):
        for dsn in self._dsn:
            await self._dsn_ready_event[dsn].wait()

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

    def pool_is_master(self, pool: PoolT) -> bool:
        return pool in self._master_pool_set

    def pool_is_replica(self, pool: PoolT) -> bool:
        return pool in self._replica_pool_set

    def register_connection(self, connection: ConnT, pool: PoolT):
        self._unmanaged_connections[connection] = pool

    def get_last_response_time(self, pool: PoolT) -> Optional[float]:
        return self._stopwatch.get_time(pool)

    async def _clear(self):
        self._balancer = None
        await self._health.stop()

        release_tasks = []
        for connection in self._unmanaged_connections:
            release_tasks.append(self.release(connection))

        await asyncio.gather(*release_tasks, return_exceptions=True)

        self._unmanaged_connections.clear()
        self._master_pool_set.clear()
        self._replica_pool_set.clear()

    async def _periodic_pool_check(
        self,
        pool: PoolT,
        dsn: Dsn,
        sys_connection: ConnT,
    ):
        while not self._closing:
            try:
                await asyncio.wait_for(
                    self._refresh_pool_role(pool, dsn, sys_connection),
                    timeout=self._refresh_timeout,
                )
                await self._health._notify_about_pool_has_checked(dsn)
            except asyncio.TimeoutError:
                logger.warning(
                    "Periodic pool check failed for dsn=%s",
                    dsn.with_(password="******"),
                )
                self._remove_pool_from_master_set(pool, dsn)
                self._remove_pool_from_replica_set(pool, dsn)
                await self._health._notify_about_pool_has_checked(dsn)

            await asyncio.sleep(self._refresh_delay)

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

    async def _refresh_pool_role(
        self,
        pool: PoolT,
        dsn: Dsn,
        sys_connection: ConnT,
    ):
        with self._stopwatch(pool):
            is_master = await self._is_master(sys_connection)
        if is_master:
            await self._add_pool_to_master_set(pool, dsn)
            self._remove_pool_from_replica_set(pool, dsn)
        else:
            await self._add_pool_to_replica_set(pool, dsn)
            self._remove_pool_from_master_set(pool, dsn)
        self._dsn_ready_event[dsn].set()

    def __iter__(self):
        return chain(iter(self._master_pool_set), iter(self._replica_pool_set))

    async def __aenter__(self):
        await self.ready()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ("BasePoolManager",)
