import asyncio
import logging
from collections.abc import Sequence
from typing import (
    Generic,
    TypeVar,
)

from .abc import PoolDriver
from .acquire import PoolAcquireContext
from .balancer_policy.base import AbstractBalancerPolicy
from .balancer_policy.greedy import GreedyBalancerPolicy
from .exceptions import PoolManagerClosedError
from .constants import (
    DEFAULT_ACQUIRE_TIMEOUT,
    DEFAULT_MASTER_AS_REPLICA_WEIGHT,
    DEFAULT_REFRESH_DELAY,
    DEFAULT_REFRESH_TIMEOUT,
    DEFAULT_STOPWATCH_WINDOW_SIZE,
)
from .health import PoolHealthMonitor
from .metrics import (
    CalculateMetrics,
    HasqlGauges,
    Metrics,
    PoolMetrics,
    PoolRole,
)
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

        self._acquire_timeout = acquire_timeout
        self._refresh_delay = refresh_delay
        self._refresh_timeout = refresh_timeout
        self._fallback_master = fallback_master
        self._master_as_replica_weight = master_as_replica_weight
        self._unmanaged_connections: dict[ConnT, PoolT] = {}
        self._metrics = CalculateMetrics()
        self._closing = False
        self._closed = False

        self._health: PoolHealthMonitor[PoolT, ConnT] = PoolHealthMonitor(
            pool_state=self._pool_state,
            refresh_delay=refresh_delay,
            refresh_timeout=refresh_timeout,
            closing_getter=lambda: self._closing,
        )

    # --- Public pool-state proxy properties ---

    @property
    def dsn(self) -> Sequence[Dsn]:
        return self._pool_state.dsn

    @property
    def master_pool_count(self) -> int:
        return self._pool_state.master_pool_count

    @property
    def replica_pool_count(self) -> int:
        return self._pool_state.replica_pool_count

    @property
    def available_pool_count(self) -> int:
        return self._pool_state.available_pool_count

    @property
    def pools(self) -> Sequence[PoolT | None]:
        return self._pool_state.pools

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def balancer(self) -> AbstractBalancerPolicy[PoolT] | None:
        return self._balancer

    @property
    def refresh_delay(self) -> float:
        return self._refresh_delay

    @property
    def refresh_timeout(self) -> float:
        return self._refresh_timeout

    # --- Public pool-state proxy methods ---

    def pool_is_master(self, pool: PoolT) -> bool:
        return self._pool_state.pool_is_master(pool)

    def pool_is_replica(self, pool: PoolT) -> bool:
        return self._pool_state.pool_is_replica(pool)

    def get_pool_freesize(self, pool: PoolT) -> int:
        return self._pool_state.get_pool_freesize(pool)

    def get_last_response_time(self, pool: PoolT) -> float | None:
        return self._pool_state.get_last_response_time(pool)

    async def get_master_pools(self) -> list[PoolT]:
        return await self._pool_state.get_master_pools()

    async def get_replica_pools(
        self, fallback_master: bool = False,
    ) -> list[PoolT]:
        return await self._pool_state.get_replica_pools(
            fallback_master=fallback_master,
        )

    async def wait_next_pool_check(self, timeout: int = 10) -> None:
        await self._pool_state.wait_next_pool_check(timeout)

    async def wait_all_ready(self) -> None:
        await self._pool_state.wait_all_ready()

    async def wait_masters_ready(self, masters_count: int) -> None:
        await self._pool_state.wait_masters_ready(masters_count)

    async def wait_replicas_ready(self, replicas_count: int) -> None:
        await self._pool_state.wait_replicas_ready(replicas_count)

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

    # --- Metrics ---

    def metrics(self) -> Metrics:
        pool_state = self._pool_state
        pool_metrics = []
        for pool in pool_state.pools:
            if pool is None:
                continue
            stats = pool_state.pool_stats(pool)

            role: PoolRole | None
            if pool_state.pool_is_master(pool):
                role = PoolRole.MASTER
            elif pool_state.pool_is_replica(pool):
                role = PoolRole.REPLICA
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
            unavailable_count=(
                len([p for p in pool_state.pools if p is not None])
                - pool_state.available_pool_count
            ),
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
            raise PoolManagerClosedError("Pool manager is closed")

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
            raise PoolManagerClosedError("Pool manager is closed")

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

    # --- Release (public, for await-pattern users) ---

    async def release(self, connection: ConnT, **kwargs) -> None:
        pool = self._unmanaged_connections.pop(connection, None)
        if pool is None:
            return
        self._metrics.remove_connection(self._pool_state.host(pool))
        await self._pool_state.release_to_pool(connection, pool, **kwargs)

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

    async def terminate(self):
        self._closing = True
        await self._clear()
        pool_state = self._pool_state
        await asyncio.gather(
            *[
                pool_state.terminate_pool(pool)
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

    async def __aenter__(self):
        await self._pool_state.ready()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ("BasePoolManager",)
