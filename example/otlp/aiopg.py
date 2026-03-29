"""hasql + aiopg: export pool metrics to an OTLP collector.

Usage:
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    python example/otlp/aiopg.py --dsn postgresql://u:p@db1,db2/mydb

Dependencies: hasql, aiopg, opentelemetry-sdk,
              opentelemetry-exporter-otlp-proto-grpc
"""

import argparse
import asyncio

from hasql.driver.aiopg import PoolManager

from common import register_hasql_metrics, setup_meter_provider

parser = argparse.ArgumentParser()
parser.add_argument("--dsn", required=True, help="Multi-host PostgreSQL DSN")
parser.add_argument(
    "--interval", type=int, default=10, help="Export interval (s)",
)


async def main():
    args = parser.parse_args()

    provider = setup_meter_provider(export_interval_ms=args.interval * 1000)

    pool = PoolManager(
        args.dsn,
        fallback_master=True,
        pool_factory_kwargs={"minsize": 2, "maxsize": 10},
    )
    await pool.pool_state.ready()
    register_hasql_metrics(pool)

    print(f"Exporting metrics every {args.interval}s. Press Ctrl+C to stop.")
    try:
        while True:
            async with pool.acquire_master() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        await pool.close()
        provider.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
