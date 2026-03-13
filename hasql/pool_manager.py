import asyncio
import logging
from types import MappingProxyType
from typing import (
    Dict,
    Generic,
    Optional,
    TypeVar,
)

from .abc import PoolDriver
from .acquire import AcquireContext, PoolAcquireContext
from .balancer_policy.base import AbstractBalancerPolicy
from .balancer_policy.greedy import GreedyBalancerPolicy
from .constants import (
    DEFAULT_ACQUIRE_TIMEOUT,
    DEFAULT_MASTER_AS_REPLICA_WEIGHT,
    DEFAULT_REFRESH_DELAY,
    DEFAULT_REFRESH_TIMEOUT,
    DEFAULT_STOPWATCH_WINDOW_SIZE,
)
from .health import PoolHealthMonitor
from .metrics import CalculateMetrics, HasqlGauges, Metrics, PoolMetrics
from .pool_state import PoolState
from .utils import Dsn, split_dsn

logger = logging.getLogger(__name__)

PoolT = TypeVar("PoolT")
ConnT = TypeVar("ConnT")


class BasePoolManager(Generic[PoolT, ConnT]):
    _unmanaged_connections: Dict[ConnT, PoolT]

    def __init__(
        self,
        dsn: str,
        *,
        driver: PoolDriver[PoolT, ConnT],
        acquire_timeout: float = DEFAULT_ACQUIRE_TIMEOUT,
        refresh_delay: float = DEFAULT_REFRESH_DELAY,
        refresh_timeout: float = DEFAULT_REFRESH_TIMEOUT,
        fallback_master: bool = False,
        master_as_replica_weight: float = DEFAULT_MASTER_AS_REPLICA_WEIGHT,
        balancer_policy: type[AbstractBalancerPolicy] = GreedyBalancerPolicy,
        stopwatch_window_size: int = DEFAULT_STOPWATCH_WINDOW_SIZE,
        pool_factory_kwargs: Optional[dict] = None,
    ):
        if not issubclass(balancer_policy, AbstractBalancerPolicy):
            raise ValueError(
                "balancer_policy must be a class BaseBalancerPolicy heir",
            )

        if pool_factory_kwargs is None:
            pool_factory_kwargs = {}

        self._driver = driver
        self._pool_factory_kwargs = MappingProxyType(
            self._driver.prepare_pool_factory_kwargs(pool_factory_kwargs),
        )

        self.pool_state: PoolState[PoolT, ConnT] = PoolState(
            dsn_list=split_dsn(dsn),
            driver=driver,
            stopwatch_window_size=stopwatch_window_size,
        )

        self._balancer: Optional[AbstractBalancerPolicy[PoolT]] = (
            balancer_policy(self.pool_state)
        )
        self._health: PoolHealthMonitor[PoolT, ConnT] = PoolHealthMonitor(self)

        self._acquire_timeout = acquire_timeout
        self._refresh_delay = refresh_delay
        self._refresh_timeout = refresh_timeout
        self._fallback_master = fallback_master
        self._master_as_replica_weight = master_as_replica_weight
        self._unmanaged_connections: Dict[ConnT, PoolT] = {}
        self._metrics = CalculateMetrics()
        self._closing = False
        self._closed = False

    @property
    def driver(self) -> PoolDriver[PoolT, ConnT]:
        return self._driver

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
    def balancer(self) -> Optional[AbstractBalancerPolicy[PoolT]]:
        return self._balancer

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def closed(self) -> bool:
        return self._closed

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

    # --- End driver proxy methods ---

    def metrics(self) -> Metrics:
        pool_state = self.pool_state
        pool_metrics = []
        for pool in pool_state._pools:
            if pool is None:
                continue
            stats = self._driver.pool_stats(pool)

            if pool_state.pool_is_master(pool):
                role = "master"
            elif pool_state.pool_is_replica(pool):
                role = "replica"
            else:
                role = None

            in_flight = sum(
                1 for p in self._unmanaged_connections.values() if p is pool
            )

            pool_metrics.append(PoolMetrics(
                host=self._driver.host(pool),
                role=role,
                healthy=role is not None,
                min=stats.min,
                max=stats.max,
                idle=stats.idle,
                used=stats.used,
                response_time=pool_state.get_last_response_time(pool),
                in_flight=in_flight,
                extra=stats.extra,
            ))

        gauges = HasqlGauges(
            master_count=pool_state.master_pool_count,
            replica_count=pool_state.replica_pool_count,
            available_count=pool_state.available_pool_count,
            active_connections=len(self._unmanaged_connections),
            closing=self._closing,
            closed=self._closed,
        )

        return Metrics(
            pools=pool_metrics,
            hasql=self._metrics.metrics(),
            gauges=gauges,
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

    def register_connection(self, connection: ConnT, pool: PoolT):
        self._unmanaged_connections[connection] = pool

    def unregister_connection(self, connection: ConnT) -> None:
        self._unmanaged_connections.pop(connection, None)

    async def close(self):
        self._closing = True
        await self._clear()
        await asyncio.gather(
            *[
                self._close(pool)
                for pool in self.pool_state._pools
                if pool is not None
            ],
            return_exceptions=True,
        )
        self._closing = False
        self._closed = True

    async def terminate(self):
        self._closing = True
        await self._clear()
        for pool in self.pool_state._pools:
            if pool is None:
                continue
            await self._terminate(pool)
        self._closing = False
        self._closed = True

    async def _clear(self):
        self._balancer = None
        await self._health.stop()

        release_tasks = []
        for connection in self._unmanaged_connections:
            release_tasks.append(self.release(connection))

        await asyncio.gather(*release_tasks, return_exceptions=True)

        self._unmanaged_connections.clear()
        self.pool_state._master_pool_set.clear()
        self.pool_state._replica_pool_set.clear()

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
                self.pool_state._remove_pool_from_master_set(pool, dsn)
                self.pool_state._remove_pool_from_replica_set(pool, dsn)
                await self._health._notify_about_pool_has_checked(dsn)

            await asyncio.sleep(self._refresh_delay)

    async def _refresh_pool_role(
        self,
        pool: PoolT,
        dsn: Dsn,
        sys_connection: ConnT,
    ):
        with self.pool_state._stopwatch(pool):
            is_master = await self._is_master(sys_connection)
        if is_master:
            await self.pool_state._add_pool_to_master_set(pool, dsn)
            self.pool_state._remove_pool_from_replica_set(pool, dsn)
        else:
            await self.pool_state._add_pool_to_replica_set(pool, dsn)
            self.pool_state._remove_pool_from_master_set(pool, dsn)
        self.pool_state._dsn_ready_event[dsn].set()

    def __iter__(self):
        return iter(self.pool_state)

    async def __aenter__(self):
        await self.pool_state.ready()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ("BasePoolManager",)
