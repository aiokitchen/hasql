.. image:: https://raw.githubusercontent.com/aiokitchen/hasql/master/resources/logo.svg
   :width: 365
   :height: 265

hasql
=====

``hasql`` is a library for acquiring actual connections to masters and replicas
in high available PostgreSQL clusters.

.. image:: https://raw.githubusercontent.com/aiokitchen/hasql/master/resources/diagram.svg

Features
========

* completely asynchronous api
* automatic detection of the host role in the cluster
* health-checks for each host and automatic traffic outage for
  unavailable hosts
* autodetection of hosts role changes, in case replica
  host will be promoted to master
* different policies for load balancing
* support for ``asyncpg``, ``psycopg3``, ``aiopg``, ``sqlalchemy`` and ``asyncpgsa``


Usage
=====

Some useful examples

Creating connection pool
************************

When acquiring a connection, the connection object of the used driver is
returned (``aiopg.connection.Connection`` for **aiopg** and
``asyncpg.pool.PoolConnectionProxy`` for **asyncpg** and **asyncpgsa**)


Database URL specirication rules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Multiple hosts should be passed comma separated

  * multihost example:

    * ``postgresql://db1,db2,db3/``
  * split result:

    * ``postgresql://db1:5432/``
    * ``postgresql://db2:5432/``
    * ``postgresql://db3:5432/``
* The non-default port for each host might be passed after hostnames. e.g.

  * multihost example:

    * ``postgresql://db1:1234,db2:5678,db3/``
  * split result:

    * ``postgresql://db1:1234/``
    * ``postgresql://db2:5678/``
    * ``postgresql://db3:5432/``
* The special case for non-default port for all hosts

  * multihost example:

    * ``postgresql://db1,db2,db3:6432/``
  * split result:

    * ``postgresql://db1:6432/``
    * ``postgresql://db2:6432/``
    * ``postgresql://db3:6432/``


For ``aiopg`` or ``aiopg.sa``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**aiopg** must be installed as a requirement.

Code example using ``aiopg``:

.. code-block:: python

    from hasql.aiopg import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool

Code example using ``aiopg.sa``:

.. code-block:: python

    from hasql.aiopg_sa import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool

For ``asyncpg``
~~~~~~~~~~~~~~~

**asyncpg** must be installed as a requirement

.. code-block:: python

    from hasql.asyncpg import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool

For ``sqlalchemy``
~~~~~~~~~~~~~~~~~~

**sqlalchemy[asyncio] & asyncpg** must be installed as requirements

.. code-block:: python

    from hasql.asyncsqlalchemy import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"


    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(
            multihost_dsn,

            # Use master for acquire_replica, if no replicas available
            fallback_master=True,

            # You can pass pool-specific options
            pool_factory_kwargs=dict(
                pool_size=10,
                max_overflow=5
            )
        )

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool


For ``asyncpgsa``
~~~~~~~~~~~~~~~~~

**asyncpgsa** must be installed as a requirement

.. code-block:: python

    from hasql.asyncpgsa import PoolManager

    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])

    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool


For ``psycopg3``
~~~~~~~~~~~~~~~~

**psycopg3** must be installed as a requirement (package name is `psycopg`)
Configure queue limits explicitly with
``pool_factory_kwargs={"max_waiting": ...}`` if you want
``psycopg_pool.TooManyRequests`` on pool saturation. Otherwise the driver
default queue behavior is used.

.. code-block:: python

    from hasql.psycopg3 import PoolManager


    hosts = ",".join([
        "master-host:5432",
        "replica-host-1:5432",
        "replica-host-2:5432",
    ])
    multihost_dsn = f"postgresql://user:password@{hosts}/dbname"

    async def create_pool(dsn) -> PoolManager:
        pool = PoolManager(multihost_dsn)

        # Waiting for 1 master and 1 replica will be available
        await pool.ready(masters_count=1, replicas_count=1)
        return pool


Acquiring connections
*********************

Connections should be acquired with async context manager:

Acquiring master connection
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        async with pool.acquire(read_only=False) as connection:
            ...

or

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        async with pool.acquire_master() as connection:
            ...

Acquiring replica connection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        async with pool.acquire(read_only=True) as connection:
            ...

or

.. code-block:: python

    async def do_something():
        pool = await create_pool(multihost_dsn)
        async with pool.acquire_replica() as connection:
            ...

How it works?
=============

For each host from dsn string, a connection pool is created. From each pool one
connection is reserved, which is used to check the availability of the host and
its role. The minimum and maximum number of connections in the pool increases
by 1 (to reserve a system connection).

For each pool a background task is created, in which the host availability and
its role (master or replica) is checked once every `refresh_delay` second.

When switching hosts roles, hasql detects this with a slight delay.

For PostgreSQL, when switching the master, all connections to all hosts are
broken (the details of implementing PostgreSQL).

If there are no available hosts, the methods acquire(), acquire_master(), and
acquire_replica() wait until the host with the desired role startup.

Balancer Policies
*****************

When multiple pools match the requested role (e.g. several healthy replicas),
hasql uses a balancer policy to choose which pool to acquire a connection from.
The policy is set via the ``balancer_policy`` parameter of ``PoolManager``.

``GreedyBalancerPolicy`` (default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Picks the pool with the most free connections. When several pools are tied,
chooses randomly among them.

Best for workloads where you want to fill up idle pools first and avoid
acquiring from pools that are already under pressure.

``RoundRobinBalancerPolicy``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cycles through available pools in order, giving each pool an equal share
of requests regardless of pool state or host performance.

Best for uniform workloads where all replicas have similar hardware and
you want simple, predictable distribution.

``RandomWeightedBalancerPolicy``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Selects a pool randomly with probability proportional to response time —
faster hosts get more connections, slower hosts get fewer, but no host is
completely starved.

Best for heterogeneous clusters where replicas differ in hardware, network
latency, or current load. Unlike Greedy, it avoids thundering herd problems
by distributing requests probabilistically instead of always picking the
single "best" pool.

.. list-table:: Policy Comparison
   :header-rows: 1
   :widths: 25 25 25 25

   * - Property
     - Greedy
     - RoundRobin
     - RandomWeighted
   * - Selection strategy
     - Most free connections
     - Sequential rotation
     - Weighted by response time
   * - Adapts to load
     - Yes (pool state)
     - No
     - Yes (latency)
   * - Thundering herd risk
     - Higher
     - None
     - None
   * - Heterogeneous replicas
     - Poor
     - Poor
     - Good
   * - Predictability
     - Low
     - High
     - Medium
   * - Best for
     - Low-concurrency
     - Uniform clusters
     - Mixed hardware

.. code-block:: python

    from hasql.balancer_policy import (
        GreedyBalancerPolicy,
        RandomWeightedBalancerPolicy,
        RoundRobinBalancerPolicy,
    )
    from hasql.asyncpg import PoolManager

    pool = PoolManager(
        dsn,
        balancer_policy=RandomWeightedBalancerPolicy,
    )

Metrics
=======

Every ``PoolManager`` exposes a ``metrics()`` method that returns a
point-in-time snapshot of the entire cluster state.

.. code-block:: python

    m = pool_manager.metrics()

The returned ``Metrics`` object contains three layers:

``m.pools`` — per-pool metrics
******************************

A sequence of ``PoolMetrics`` dataclasses, one per database host:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field
     - Description
   * - ``host``
     - Host address of the pool
   * - ``role``
     - ``"master"``, ``"replica"``, or ``None`` (unknown)
   * - ``healthy``
     - ``True`` if the host has a known role
   * - ``min``
     - Minimum connections configured
   * - ``max``
     - Maximum connections configured
   * - ``idle``
     - Connections currently idle in the pool
   * - ``used``
     - Connections currently checked out
   * - ``response_time``
     - Last health-check round-trip time (seconds)
   * - ``in_flight``
     - Connections acquired through the pool manager
   * - ``extra``
     - Driver-specific data (e.g. psycopg3's ``requests_waiting``,
       SQLAlchemy's ``overflow``)

``m.gauges`` — cluster-wide gauges
***********************************

A ``HasqlGauges`` dataclass with aggregate state:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Description
   * - ``master_count``
     - Number of detected masters
   * - ``replica_count``
     - Number of detected replicas
   * - ``available_count``
     - Total pools with a known role
   * - ``active_connections``
     - Connections currently held by application code
   * - ``closing``
     - ``True`` while the pool manager is shutting down
   * - ``closed``
     - ``True`` after shutdown is complete

``m.hasql`` — internal counters
*******************************

A ``HasqlMetrics`` dataclass with cumulative acquire/release counters
and timing data, useful for tracking pool manager overhead.

Example: simple metrics endpoint
********************************

.. code-block:: python

    from dataclasses import asdict
    import json

    async def handle_metrics(request):
        m = pool_manager.metrics()
        return web.json_response(asdict(m))


Exporting metrics to OTLP
==========================

hasql ships with ready-to-use examples for exporting metrics to any
OpenTelemetry-compatible collector (Prometheus, Grafana, Datadog, etc.)
via OTLP gRPC.

Quick start
***********

Install the OpenTelemetry dependencies:

.. code-block:: bash

    pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc

Use the helper from ``example/otlp/common.py``:

.. code-block:: python

    from hasql.asyncpg import PoolManager
    from example.otlp.common import (
        register_hasql_metrics,
        setup_meter_provider,
    )

    provider = setup_meter_provider(export_interval_ms=10_000)

    pool = PoolManager(dsn, fallback_master=True)
    await pool.ready()

    # Registers observable gauges — OTel calls metrics()
    # automatically at each export interval
    register_hasql_metrics(pool)

Exported OTel gauges
********************

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Gauge name
     - Labels
     - Source
   * - ``db.pool.connections.min``
     - ``host``, ``role``
     - ``PoolMetrics.min``
   * - ``db.pool.connections.max``
     - ``host``, ``role``
     - ``PoolMetrics.max``
   * - ``db.pool.connections.idle``
     - ``host``, ``role``
     - ``PoolMetrics.idle``
   * - ``db.pool.connections.used``
     - ``host``, ``role``
     - ``PoolMetrics.used``
   * - ``db.pool.connections.in_flight``
     - ``host``, ``role``
     - ``PoolMetrics.in_flight``
   * - ``db.pool.healthy``
     - ``host``, ``role``
     - ``PoolMetrics.healthy``
   * - ``db.pool.health_check.duration``
     - ``host``, ``role``
     - ``PoolMetrics.response_time``
   * - ``db.pool.masters``
     - —
     - ``HasqlGauges.master_count``
   * - ``db.pool.replicas``
     - —
     - ``HasqlGauges.replica_count``
   * - ``db.pool.active_connections``
     - —
     - ``HasqlGauges.active_connections``
   * - ``db.pool.extra.<key>``
     - ``host``, ``role``
     - ``PoolMetrics.extra[key]``

Driver-specific extras
**********************

Some drivers expose additional pool internals via ``PoolMetrics.extra``.
Use ``register_extra_gauges()`` to export them as OTel gauges:

.. code-block:: python

    from example.otlp.common import register_extra_gauges

    # psycopg3: queue depth, error counters, etc.
    register_extra_gauges(pool, [
        "pool_size", "requests_waiting", "connections_errors",
    ])

    # SQLAlchemy: overflow connections
    register_extra_gauges(pool, ["overflow"])

Per-driver examples live in ``example/otlp/``.

Dashboard recommendations
*************************

The exported metrics map well to Grafana / Datadog dashboard panels:

**Cluster health overview**

* ``db.pool.masters`` / ``db.pool.replicas`` — single-stat panels;
  alert when master drops to 0 or replicas drop below expected count
* ``db.pool.healthy`` by ``host`` — table or status map showing
  per-host health; any 0 value means the host lost its role

**Connection pool utilization**

* ``db.pool.connections.used`` / ``db.pool.connections.max`` by
  ``host`` — saturation ratio; alert when approaching 100%
* ``db.pool.connections.idle`` by ``host`` — if consistently 0, the
  pool is undersized
* ``db.pool.connections.in_flight`` by ``host`` — connections held by
  application code right now; spikes indicate slow queries or leaked
  connections

**Latency and performance**

* ``db.pool.health_check.duration`` by ``host`` — time series;
  rising latency on a replica can predict upcoming failover
* Compare ``response_time`` across hosts to spot slow replicas
  before they affect user traffic

**Pool manager overhead**

* ``db.pool.active_connections`` — total connections held across all
  pools; correlate with application request rate to right-size pools

**Driver-specific panels (psycopg3)**

* ``db.pool.extra.requests_waiting`` — queue depth; sustained > 0
  means the pool is saturated
* ``db.pool.extra.connections_errors`` — connection failures;
  alert on rate increase

**Alerting rules**

* ``db.pool.masters == 0`` — **critical**: no master available
* ``db.pool.replicas == 0`` — **warning**: all reads will fall back
  to master (if ``fallback_master=True``) or fail
* ``db.pool.connections.used / db.pool.connections.max > 0.9`` —
  **warning**: pool near exhaustion
* ``db.pool.health_check.duration > threshold`` — **warning**: host
  becoming slow, may lose role soon
* ``db.pool.extra.requests_waiting > 0`` for sustained period —
  **warning**: pool undersized for current load


Architecture
============

hasql uses a composition-based architecture. Pool orchestration logic lives in
``BasePoolManager``, while all driver-specific operations (creating pools,
acquiring/releasing connections, checking master status) are encapsulated in
``PoolDriver`` implementations.

.. code-block::

    PoolDriver (ABC)                    <- driver interface (10 methods)
      ├── AiopgDriver
      │     └── AiopgSaDriver
      ├── AsyncpgDriver
      │     └── AsyncpgsaDriver
      ├── Psycopg3Driver
      └── AsyncSqlAlchemyDriver

    BasePoolManager (concrete)          <- has-a PoolDriver
      └── driver-specific PoolManager   <- thin wrapper: creates driver

Each driver-specific ``PoolManager`` (e.g. ``hasql.aiopg.PoolManager``) is a
thin subclass that passes the appropriate ``PoolDriver`` instance to
``BasePoolManager``:

.. code-block:: python

    from hasql.aiopg import PoolManager

    # PoolManager internally creates AiopgDriver and passes it
    # to BasePoolManager — no need to interact with PoolDriver directly
    pool = PoolManager("postgresql://master,replica/db")

Custom drivers
**************

You can implement a custom driver by subclassing ``PoolDriver``:

.. code-block:: python

    from hasql.abc import PoolDriver
    from hasql.pool_manager import BasePoolManager

    class MyDriver(PoolDriver[MyPool, MyConnection]):
        # implement all abstract methods ...
        ...

    pool = BasePoolManager(
        "postgresql://master,replica/db",
        driver=MyDriver(),
    )

Overview
========

* hasql.abc.PoolDriver
    Abstract base class for database driver implementations.
    Each driver must implement:

    * ``get_pool_freesize(pool)`` - Return number of free connections
    * ``acquire_from_pool(pool, *, timeout, **kwargs)`` - Acquire a
      connection
    * ``release_to_pool(connection, pool, **kwargs)`` - Release a
      connection
    * ``is_master(connection)`` - Check if connection is to master
    * ``pool_factory(dsn, **kwargs)`` - Create a connection pool
    * ``close_pool(pool)`` - Gracefully close a pool
    * ``terminate_pool(pool)`` - Forcefully terminate a pool
    * ``is_connection_closed(connection)`` - Check if connection is closed
    * ``host(pool)`` - Return host address for a pool
    * ``pool_stats(pool)`` - Return ``PoolStats`` for a single pool

    Optional override:

    * ``prepare_pool_factory_kwargs(kwargs)`` - Adjust pool factory kwargs
      (e.g. to reserve a system connection by incrementing min/max size)

* hasql.pool_manager.BasePoolManager
    * ``__init__(dsn, *, driver, acquire_timeout, refresh_delay, refresh_timeout, fallback_master, master_as_replica_weight, balancer_policy, pool_factory_kwargs)``:

        * ``dsn: str`` - Connection string used by the connection.

        * ``driver: PoolDriver`` - Driver instance that implements
          database-specific pool operations. Driver-specific
          ``PoolManager`` classes provide this automatically.

        * ``acquire_timeout: Union[int, float]`` - Default timeout
          (in seconds) for connection operations. 1 sec by default.

        * ``refresh_delay: Union[int, float]`` - Delay time (in seconds)
          between host polls. 1 sec by default.

        * ``refresh_timeout: Union[int, float]`` - Timeout (in seconds)
          for trying to connect and get the host role. 30 sec by
          default.

        * ``fallback_master: bool`` - Use connections from master if
          replicas are missing. False by default.

        * ``master_as_replica_weight: float`` - Probability of using
          the master as a replica (from 0. to 1.; 0. - master is not
          used as a replica; 1. - master can be used as a replica).

        * ``balancer_policy: type`` - Connection pool balancing policy
          (``GreedyBalancerPolicy``,
          ``RandomWeightedBalancerPolicy`` or
          ``RoundRobinBalancerPolicy``).

        * ``stopwatch_window_size: int`` - Window size for calculating
          the median response time of each pool.

        * ``pool_factory_kwargs: Optional[dict]`` - Connection pool
          creation parameters that are passed to pool factory.

    * coroutine async-with
      ``acquire(read_only, fallback_master, timeout, **kwargs)``
      Acquire a connection from free pool.

        * ``readonly: bool`` - ``True`` if need return connection to
          replica, ``False`` - to master. False by default.

        * ``fallback_master: Optional[bool]`` - Use connections from
          master if replicas are missing. If None, then the default
          value is used.

        * ``master_as_replica_weight: float`` - Probability of using
          the master as a replica (from 0. to 1.).

        * ``timeout: Union[int, float]`` - Timeout (in seconds) for
          connection operations.

        * ``kwargs`` - Arguments to be passed to the pool acquire()
          method.

    * coroutine async-with ``acquire_master(timeout, **kwargs)``
      Acquire a connection from free master pool.
      Equivalent ``acquire(read_only=False)``

        * ``timeout: Union[int, float]`` - Timeout (in seconds) for
          connection operations.

        * ``kwargs`` - Arguments to be passed to the pool acquire()
          method.

    * coroutine async-with
      ``acquire_replica(fallback_master, timeout, **kwargs)``
      Acquire a connection from free replica pool.
      Equivalent ``acquire(read_only=True)``

        * ``fallback_master: Optional[bool]`` - Use connections from
          master if replicas are missing. If None, then the default
          value is used.

        * ``master_as_replica_weight: float`` - Probability of using
          the master as a replica (from 0. to 1.).

        * ``timeout: Union[int, float]`` - Timeout (in seconds) for
          connection operations.

        * ``kwargs`` - Arguments to be passed to the pool acquire()
          method.

    * coroutine ``close()``
      Close pool. Mark all pool connections to be closed on getting
      back to pool. Closed pool doesn’t allow to acquire new
      connections.

    * ``metrics()``
      Returns a ``Metrics`` snapshot of the entire cluster state.

    * coroutine ``ready(masters_count, replicas_count, timeout)``
      Waiting for a connection to the database hosts. If
      masters_count is ``None`` and replicas_count is None, then
      connection to all hosts is expected.

        * ``masters_count: Optional[int]`` - Minimum number of master
          hosts. ``None`` by default.

        * ``replicas_count: Optional[int]`` - Minimum number of
          replica hosts. ``None`` by default.

        * ``timeout: Union[int, float]`` - Timeout for database
          connections. 10 seconds by default.

    * coroutine ``wait_masters_ready(masters_count)``
      Waiting for connection to the specified number of
      database master servers.

        * ``masters_count: int`` - Minimum number of master hosts.

    * ``available_pool_count``
      Property returning the total number of pools with a known role
      (masters + replicas).

* ``hasql.aiopg.PoolManager`` (driver: ``AiopgDriver``)

* ``hasql.aiopg_sa.PoolManager`` (driver: ``AiopgSaDriver``)

* ``hasql.asyncpg.PoolManager`` (driver: ``AsyncpgDriver``)

* ``hasql.asyncpgsa.PoolManager`` (driver: ``AsyncpgsaDriver``)

* ``hasql.asyncsqlalchemy.PoolManager`` (driver: ``AsyncSqlAlchemyDriver``)

* ``hasql.psycopg3.PoolManager`` (driver: ``Psycopg3Driver``)

Balancer policies
=================

* ``hasql.balancer_policy.GreedyBalancerPolicy``
  Chooses pool with the most free connections. If there are several such pools,
  a random one is taken.

* ``hasql.balancer_policy.RandomWeightedBalancerPolicy``
  Chooses random pool according to their weights. The weight is inversely
  proportional to the response time of the database of the respective pool 
  (faster response - higher weight).

* ``hasql.balancer_policy.RoundRobinBalancerPolicy``
