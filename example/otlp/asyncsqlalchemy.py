"""hasql + SQLAlchemy async: export pool metrics to an OTLP collector.

Demonstrates exporting the SQLAlchemy-specific ``overflow`` extra gauge.

Usage:
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    python -m example.otlp.asyncsqlalchemy --dsn postgresql://u:p@db1,db2/mydb

Dependencies: hasql, sqlalchemy[asyncio], asyncpg, opentelemetry-sdk,
              opentelemetry-exporter-otlp-proto-grpc
"""

import argparse
import asyncio

import sqlalchemy as sa

from hasql.driver.asyncsqlalchemy import PoolManager

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
            pool_factory_kwargs={"pool_size": 10},
        )
        try:
            await pool.ready()
            async with observe_hasql_metrics(pool, extra_keys=("overflow",)):
                print(
                    f"Exporting metrics every {args.interval}s. "
                    "Press Ctrl+C to stop.",
                )
                while True:
                    async with pool.acquire_master() as conn:
                        await conn.execute(sa.text("SELECT 1"))
                    await asyncio.sleep(1)
        finally:
            await pool.close()
    finally:
        await asyncio.to_thread(provider.shutdown)


if __name__ == "__main__":
    asyncio.run(main())
