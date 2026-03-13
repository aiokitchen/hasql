# Metrics Dashboard Example

This guide shows how to build a Grafana dashboard for a hasql-managed PostgreSQL
cluster using the OTLP metrics exported by the helpers in `example/otlp/`.

The same metrics work with any OpenTelemetry-compatible backend (Prometheus,
Datadog, New Relic, etc.) — only the query syntax differs.

## Prerequisites

1. A running hasql application exporting metrics via OTLP (see `example/otlp/`)
2. An OpenTelemetry Collector forwarding to your metrics backend
3. Grafana (or equivalent) connected to that backend

## Metric reference

All gauges are registered by `register_hasql_metrics()` from
`example/otlp/common.py`. Driver-specific extras require an additional
`register_extra_gauges()` call.

| Gauge name | Labels | Description |
|---|---|---|
| `db.pool.connections.min` | `host`, `role` | Configured minimum pool size |
| `db.pool.connections.max` | `host`, `role` | Configured maximum pool size |
| `db.pool.connections.idle` | `host`, `role` | Connections sitting idle in the pool |
| `db.pool.connections.used` | `host`, `role` | Connections currently checked out by the driver |
| `db.pool.connections.in_flight` | `host`, `role` | Connections held by application code via pool manager |
| `db.pool.healthy` | `host`, `role` | 1 if the host has a known role, 0 otherwise |
| `db.pool.health_check.duration` | `host`, `role` | Last health-check round-trip time (seconds) |
| `db.pool.masters` | — | Number of detected master hosts |
| `db.pool.replicas` | — | Number of detected replica hosts |
| `db.pool.active_connections` | — | Total connections held across all pools |
| `db.pool.extra.<key>` | `host`, `role` | Driver-specific extras (see below) |

### Driver-specific extra keys

**psycopg3** (`register_extra_gauges(pool, [...])`):

| Key | Description |
|---|---|
| `pool_size` | Current number of connections in the pool |
| `requests_waiting` | Clients waiting for a connection |
| `requests_num` | Total requests served |
| `requests_errors` | Requests that failed |
| `connections_num` | Total connections created |
| `connections_errors` | Connection attempts that failed |
| `connections_lost` | Connections lost unexpectedly |
| `returns_bad` | Connections returned in bad state |
| `usage_ms` | Total connection usage time (ms) |

**SQLAlchemy** (`register_extra_gauges(pool, ["overflow"])`):

| Key | Description |
|---|---|
| `overflow` | Connections beyond `pool_size` currently active |

---

## Dashboard layout

### Row 1 — Cluster Health

The top row answers: "Is everything OK?"

#### Panel 1.1: Master Count (Stat)

Single-value panel showing the number of available masters.

```
Metric: db.pool.masters
Display: Stat (single value)
Thresholds:
  - 1  → green
  - 0  → red
```

A healthy cluster always shows 1. Zero means no master is detected —
all writes will block until a master appears.

#### Panel 1.2: Replica Count (Stat)

```
Metric: db.pool.replicas
Display: Stat (single value)
Thresholds:
  - >= expected replica count → green
  - 0                        → red
```

When replicas drop to 0, read traffic either falls back to the master
(if `fallback_master=True`) or fails entirely.

#### Panel 1.3: Host Health Map (Table)

Per-host health status at a glance.

```
Metric: db.pool.healthy
Group by: host, role
Display: Table or Status map
Mapping: 1 → "UP" (green), 0 → "DOWN" (red)
```

This panel shows every host in the DSN and whether hasql considers it
healthy. A host is healthy when its role (master or replica) has been
successfully detected by the background health checker.

---

### Row 2 — Connection Pool Utilization

The second row answers: "Do we need to scale the pools?"

#### Panel 2.1: Pool Saturation (Stacked Bar / Time Series)

Shows how each pool's capacity is distributed.

```
Metrics (per host):
  - db.pool.connections.used   → "Used" (orange)
  - db.pool.connections.idle   → "Idle" (green)
  - db.pool.connections.max - used - idle → "Reserved" (gray)
Group by: host
Display: Stacked bar chart or stacked area
```

When the "Used" segment fills the entire bar, the pool is exhausted and
new `acquire()` calls will block until a connection is returned.

**PromQL example (saturation ratio):**

```promql
db_pool_connections_used
  / db_pool_connections_max
```

Alert when this exceeds 0.9 for more than 1 minute.

#### Panel 2.2: In-Flight Connections (Time Series)

```
Metric: db.pool.connections.in_flight
Group by: host
Display: Time series
```

`in_flight` counts connections that application code has acquired through
the pool manager and not yet released. Spikes indicate:

- Long-running queries
- Leaked connections (acquired but never released/exited context manager)
- Bursts of concurrent requests

Correlate with your application's request rate to distinguish normal
load from pathological behavior.

#### Panel 2.3: Total Active Connections (Stat)

```
Metric: db.pool.active_connections
Display: Stat (single value)
```

Sum of all in-flight connections across every pool. Useful as a
top-level indicator of overall database pressure.

---

### Row 3 — Latency & Performance

The third row answers: "Is something degrading?"

#### Panel 3.1: Health Check Latency (Time Series)

The most valuable early-warning signal in this dashboard.

```
Metric: db.pool.health_check.duration
Group by: host
Display: Time series (seconds)
Y-axis: 0 → auto
```

hasql checks each host's role every `refresh_delay` seconds (default: 1s)
by running a query over the reserved system connection. The round-trip
time is recorded as `response_time` on `PoolMetrics` and exported as
`db.pool.health_check.duration`.

**What to look for:**

- **Baseline shift** — if a replica's latency gradually rises from 1ms
  to 50ms, it may be under disk/CPU pressure and could lose its role
  soon.
- **Sudden spike on one host** — network issue or the host is swapping.
- **All hosts spike simultaneously** — likely a network partition or
  shared storage problem.
- **One host disappears from the graph** — it dropped out of the healthy
  set entirely (check Panel 1.3).

**PromQL alert example:**

```promql
db_pool_health_check_duration > 0.5
```

Alert when any host's health check exceeds 500ms for more than 2 minutes.

#### Panel 3.2: Latency vs. Used Connections (Overlay)

Overlay `health_check.duration` and `connections.used` on the same graph
(dual Y-axis) for a single host. This reveals whether latency spikes
correlate with pool pressure or are independent (hardware/network).

```
Left Y-axis:  db.pool.health_check.duration{host="replica-1"}
Right Y-axis: db.pool.connections.used{host="replica-1"}
```

---

### Row 4 — Driver-Specific Panels

These panels require `register_extra_gauges()` and are only relevant
for drivers that expose extra pool internals.

#### Panel 4.1: Queue Depth — psycopg3 (Time Series)

```
Metric: db.pool.extra.requests_waiting
Group by: host
Display: Time series
```

When `requests_waiting > 0`, clients are blocked waiting for a free
connection. This is the clearest signal that the pool is undersized
for the current workload.

**PromQL alert:**

```promql
db_pool_extra_requests_waiting > 0
```

Alert when sustained for more than 30 seconds.

#### Panel 4.2: Connection Errors — psycopg3 (Time Series, Rate)

```
Metric: db.pool.extra.connections_errors
Group by: host
Display: Time series (use rate/increase for counter-like behavior)
```

A rising error count indicates the driver is failing to establish new
connections — possible causes: host down, max_connections reached on
PostgreSQL, network issues.

#### Panel 4.3: Overflow — SQLAlchemy (Time Series)

```
Metric: db.pool.extra.overflow
Group by: host
Display: Time series
```

SQLAlchemy's `QueuePool` allows creating connections beyond `pool_size`
up to `max_overflow`. When overflow is consistently > 0, the base
`pool_size` is too small for steady-state load.

---

## Alerting Rules Summary

| Rule | Severity | Condition | Meaning |
|---|---|---|---|
| No master | Critical | `db.pool.masters == 0` for 30s | All writes will fail |
| No replicas | Warning | `db.pool.replicas == 0` for 1m | Reads fall back to master or fail |
| Pool near exhaustion | Warning | `used / max > 0.9` for 1m | Pool running out of connections |
| Host unhealthy | Warning | `db.pool.healthy == 0` for 1m | Host lost its detected role |
| High health-check latency | Warning | `health_check.duration > 0.5s` for 2m | Host may be degrading |
| Queue depth | Warning | `requests_waiting > 0` for 30s | Pool undersized (psycopg3) |
| Connection errors rising | Warning | `connections_errors` rate > 0 for 1m | Driver can't connect (psycopg3) |

---

## Grafana JSON model (minimal)

Below is a stripped-down Grafana dashboard JSON that you can import
directly. It assumes a Prometheus data source named `"prometheus"` and
uses the metric names as exported by the OTLP helper.

Adapt the `datasource` and metric names to your backend.

```json
{
  "title": "hasql Pool Dashboard",
  "panels": [
    {
      "title": "Masters",
      "type": "stat",
      "gridPos": { "h": 4, "w": 4, "x": 0, "y": 0 },
      "targets": [
        { "expr": "db_pool_masters", "legendFormat": "masters" }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "steps": [
              { "color": "red", "value": null },
              { "color": "green", "value": 1 }
            ]
          }
        }
      }
    },
    {
      "title": "Replicas",
      "type": "stat",
      "gridPos": { "h": 4, "w": 4, "x": 4, "y": 0 },
      "targets": [
        { "expr": "db_pool_replicas", "legendFormat": "replicas" }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "steps": [
              { "color": "red", "value": null },
              { "color": "green", "value": 1 }
            ]
          }
        }
      }
    },
    {
      "title": "Host Health",
      "type": "table",
      "gridPos": { "h": 4, "w": 16, "x": 8, "y": 0 },
      "targets": [
        { "expr": "db_pool_healthy", "format": "table", "instant": true }
      ]
    },
    {
      "title": "Pool Saturation",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 4 },
      "targets": [
        { "expr": "db_pool_connections_used", "legendFormat": "used {{host}}" },
        { "expr": "db_pool_connections_idle", "legendFormat": "idle {{host}}" }
      ],
      "fieldConfig": {
        "defaults": { "custom": { "stacking": { "mode": "normal" } } }
      }
    },
    {
      "title": "In-Flight Connections",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 4 },
      "targets": [
        { "expr": "db_pool_connections_in_flight", "legendFormat": "{{host}}" }
      ]
    },
    {
      "title": "Health Check Latency",
      "type": "timeseries",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 12 },
      "targets": [
        { "expr": "db_pool_health_check_duration", "legendFormat": "{{host}}" }
      ],
      "fieldConfig": {
        "defaults": { "unit": "s" }
      }
    },
    {
      "title": "Active Connections (total)",
      "type": "stat",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 12 },
      "targets": [
        { "expr": "db_pool_active_connections", "legendFormat": "active" }
      ]
    }
  ],
  "schemaVersion": 39,
  "version": 1
}
```

To import: Grafana sidebar > Dashboards > Import > paste the JSON above.

---

## Full working example

See the per-driver OTLP scripts in `example/otlp/`:

| File | Driver | Extras |
|---|---|---|
| `asyncpg.py` | asyncpg | — |
| `psycopg3.py` | psycopg3 | `requests_waiting`, `connections_errors`, etc. |
| `aiopg.py` | aiopg | — |
| `aiopg_sa.py` | aiopg + SQLAlchemy | — |
| `asyncsqlalchemy.py` | SQLAlchemy async | `overflow` |

Each script creates a `PoolManager`, registers metrics, and runs a
simple workload loop so you can see data flowing into your collector.

```bash
# Start the collector (e.g. Grafana Alloy, OTel Collector, etc.)
# Then run any example:
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  python example/otlp/asyncpg.py --dsn postgresql://u:p@db1,db2/mydb
```
