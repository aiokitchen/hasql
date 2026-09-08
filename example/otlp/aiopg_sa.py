"""hasql + aiopg_sa: export pool metrics to an OTLP collector.

Usage:
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    python -m example.otlp.aiopg_sa --dsn postgresql://u:p@db1,db2/mydb

Dependencies: hasql, aiopg, opentelemetry-sdk,
              opentelemetry-exporter-otlp-proto-grpc
"""

import argparse
import asyncio

from hasql.driver.aiopg_sa import PoolManager

from .common import observe_hasql_metrics, setup_meter_provider

parser = argparse.ArgumentParser()
parser.add_argument("--dsn", required=True, help="Multi-host PostgreSQL DSN")
parser.add_argument(
    "--interval", type=int, default=10, help="Export interval (s)",
)


async def main():
    args = parser.parse_args()

    provider = setup_meter_provider(export_interval_ms=args.interval * 1000)

    try:
        pool = PoolManager(
            args.dsn,
            fallback_master=True,
        )
        try:
            await pool.ready()
            async with observe_hasql_metrics(pool):
                print(
                    f"Exporting metrics every {args.interval}s. "
                    "Press Ctrl+C to stop.",
                )
                while True:
                    async with pool.acquire_master() as conn:
                        await conn.execute("SELECT 1")
                    await asyncio.sleep(1)
        finally:
            await pool.close()
    finally:
        await asyncio.to_thread(provider.shutdown)


if __name__ == "__main__":
    asyncio.run(main())
