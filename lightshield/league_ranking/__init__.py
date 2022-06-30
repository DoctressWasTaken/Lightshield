"""League Updater Module."""
import asyncio
import json
import logging
import os
import signal

import aioredis
import asyncpg
import uvloop
from rich.progress import Progress

from lightshield.league_ranking.service import Service

uvloop.install()

if "DEBUG" in os.environ:
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
else:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
logging.debug("Debug enabled.")


class Handler:
    is_shutdown = False
    platforms = {}
    redis = postgres = None

    def __init__(self, configs):
        self.logging = logging.getLogger("Handler")
        self.api_key = None
        self.connections = configs.get("connections")
        self.config = configs.get("services")["league_ranking"]
        proxy = self.connections.get('proxy')
        self.protocol = proxy.get('protocol')
        self.proxy = "%s://%s" % (proxy.get('protocol'), proxy.get('location'))

        for platform in self.config["platform"]:
            self.platforms[platform] = Service(platform, configs, self)

    async def init(self):
        psq_con = self.connections.get("postgres")
        self.postgres = await asyncpg.create_pool(
            host=psq_con.get("hostname"),
            port=psq_con.get("port"),
            user=psq_con.get("user"),
            database=psq_con.get("database"),
            password=os.getenv(psq_con.get("password_env")),
        )

    async def shutdown(self, *args, **kwargs):
        """Initiate shutdown."""
        self.is_shutdown = True

    async def handle_shutdown(self):
        await self.postgres.close()

    async def run(self):
        """Run."""
        await self.init()
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda signame=sig: asyncio.create_task(self.shutdown())
            )
        tasks = []
        with Progress() as progress:
            for platform in self.platforms.values():
                tasks.append(asyncio.create_task(platform.run(progress)))

            await asyncio.gather(*tasks)
            await self.handle_shutdown()
