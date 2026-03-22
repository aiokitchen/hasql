import asyncio
import logging
from typing import (
    Generic,
    TypeVar,
)

from .abc import PoolDriver
from .acquire import PoolAcquireContext
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
    _unmanaged_connections: dict[ConnT, PoolT]

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
        pool_factory_kwargs: dict | None = None,
    ):
        if not issubclass(balancer_policy, AbstractBalancerPolicy):
            raise ValueError(
                "balancer_policy must be a subclass of AbstractBalancerPolicy",
            )

        self._pool_state: PoolState[PoolT, ConnT] = PoolState(
            dsn_list=split_dsn(dsn),
            driver=driver,
            stopwatch_window_size=stopwatch_window_size,
            pool_factory_kwargs=pool_factory_kwargs,
        )

        self._balancer: AbstractBalancerPolicy[PoolT] | None = (
            balancer_policy(self._pool_state)
        )
        self._health: PoolHealthMonitor[PoolT, ConnT] = PoolHealthMonitor(self)

        self._acquire_timeout = acquire_timeout
        self._refresh_delay = refresh_delay
        self._refresh_timeout = refresh_timeout
        self._fallback_master = fallback_master
        self._master_as_replica_weight = master_as_replica_weight
        self._unmanaged_connections: dict[ConnT, PoolT] = {}
        self._metrics = CalculateMetrics()
        self._closing = False
        self._closed = False

    # --- Public proxy methods/properties for pool_state ---

    async def wait_masters_ready(self, masters_count: int) -> None:
        await self._pool_state.wait_masters_ready(masters_count)

    async def ready(
        self,
        masters_count: int | None = None,
        replicas_count: int | None = None,
        timeout: int = 10,
    ) -> None:
        await self._pool_state.ready(
            masters_count=masters_count,
            replicas_count=replicas_count,
            timeout=timeout,
        )

    @property
    def available_pool_count(self) -> int:
        return self._pool_state.available_pool_count

    # --- Metrics ---

    def metrics(self) -> Metrics:
        pool_state = self._pool_state
        pool_metrics = []
        for pool in pool_state.pools:
            if pool is None:
                continue
            stats = pool_state.pool_stats(pool)

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
                host=pool_state.host(pool),
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

    # --- Acquire API ---

    def acquire(
        self,
        read_only: bool = False,
        fallback_master: bool | None = None,
        master_as_replica_weight: float | None = None,
        timeout: float | None = None,
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

        if self._balancer is None:
            raise RuntimeError("Pool manager is closed")

        return PoolAcquireContext(
            pool_state=self._pool_state,
            balancer=self._balancer,
            register_connection=self._register_connection,
            unregister_connection=self._unregister_connection,
            read_only=read_only,
            fallback_master=fallback_master,
            master_as_replica_weight=master_as_replica_weight,
            timeout=timeout,
            metrics=self._metrics,
            **kwargs,
        )

    def acquire_master(
        self,
        timeout: float | None = None,
        **kwargs,
    ) -> "PoolAcquireContext[PoolT, ConnT]":
        return self.acquire(read_only=False, timeout=timeout, **kwargs)

    def acquire_replica(
        self,
        fallback_master: bool | None = None,
        master_as_replica_weight: float | None = None,
        timeout: float | None = None,
        **kwargs,
    ) -> "PoolAcquireContext[PoolT, ConnT]":
        return self.acquire(
            read_only=True,
            fallback_master=fallback_master,
            master_as_replica_weight=master_as_replica_weight,
            timeout=timeout,
            **kwargs,
        )

    # --- Connection tracking (internal) ---

    def _register_connection(self, connection: ConnT, pool: PoolT):
        self._unmanaged_connections[connection] = pool

    def _unregister_connection(self, connection: ConnT) -> None:
        self._unmanaged_connections.pop(connection, None)

    # --- Lifecycle ---

    async def close(self):
        self._closing = True
        await self._clear()
        pool_state = self._pool_state
        await asyncio.gather(
            *[
                pool_state.close_pool(pool)
                for pool in pool_state.pools
                if pool is not None
            ],
            return_exceptions=True,
        )
        self._closing = False
        self._closed = True

    async def _clear(self):
        self._balancer = None
        await self._health.stop()

        snapshot = list(self._unmanaged_connections.items())
        self._unmanaged_connections.clear()

        release_tasks = []
        for connection, pool in snapshot:
            self._metrics.remove_connection(self._pool_state.host(pool))
            release_tasks.append(
                self._pool_state.release_to_pool(connection, pool),
            )

        await asyncio.gather(*release_tasks, return_exceptions=True)

        self._pool_state.clear_sets()

    async def _periodic_pool_check(
        self,
        pool: PoolT,
        dsn: Dsn,
        sys_connection: ConnT,
    ):
        while not self._closing:
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

    async def __aenter__(self):
        await self._pool_state.ready()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ("BasePoolManager",)
