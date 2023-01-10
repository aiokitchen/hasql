import argparse
from dataclasses import asdict

import aiohttp.web
from aiohttp.web_urldispatcher import View
from aiomisc import entrypoint
from aiomisc.service.aiohttp import AIOHTTPService

from hasql.aiopg import PoolManager

parser = argparse.ArgumentParser()
group = parser.add_argument_group('HTTP options')

group.add_argument("-l", "--address", default="::",
                   help="Listen HTTP address")
group.add_argument("-p", "--port", type=int, default=8080,
                   help="Listen HTTP port")

group.add_argument("--dsn", type=str, help="DSN to connect")
group.add_argument("--pg-maxsize", type=int, help="PG pool max size")
group.add_argument("--pg-minsize", type=int, help="PG pool min size")


class BaseView(View):
    @property
    def pool(self) -> PoolManager:
        return self.request.app['pool']


class MasterHandler(BaseView):
    async def get(self):
        async with self.pool.acquire_master(timeout=1) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                res = (await cur.fetchone())[0]
        return aiohttp.web.Response(text=str(res))


class ReplicaHandler(BaseView):
    async def get(self):
        async with self.pool.acquire_replica(timeout=1) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                res = (await cur.fetchone())[0]
        return aiohttp.web.Response(text=str(res))


class MetricsHandler(BaseView):
    async def get(self):
        metrics = self.pool.metrics()
        return aiohttp.web.json_response([asdict(m) for m in metrics])


class REST(AIOHTTPService):
    async def create_application(self) -> aiohttp.web.Application:
        app = aiohttp.web.Application()

        app.add_routes([
            aiohttp.web.get('/master', MasterHandler),
            aiohttp.web.get('/replica', ReplicaHandler),
            aiohttp.web.get('/metrics', MetricsHandler),
        ])
        pool_manager: PoolManager = PoolManager(
            arguments.dsn,
            pool_factory_kwargs=dict(
                maxsize=arguments.pg_maxsize,
                minsize=arguments.pg_minsize
            )
        )

        # Waiting for 1 master and 1 replica will be available
        await pool_manager.ready(masters_count=1, replicas_count=1)
        app['pool'] = pool_manager

        return app


if __name__ == '__main__':
    arguments = parser.parse_args()
    service = REST(address=arguments.address, port=arguments.port)

    with entrypoint(service, log_config=True) as loop:
        loop.run_forever()
