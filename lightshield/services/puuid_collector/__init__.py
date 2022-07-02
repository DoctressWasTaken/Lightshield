"""Summoner ID Updater Module."""
import asyncio
import json
import logging
import os
import signal

import aioredis
import asyncpg
import uvloop

uvloop.install()

from lightshield.services.puuid_collector.service import Platform


class Handler:
    platforms = {}
    is_shutdown = False

    def __init__(self, configs):
        self.logging = logging.getLogger("Handler")
        self.connections = configs.get("connections")
        self.config = configs.get("services")["puuid_collector"]
        proxy = self.connections.get('proxy')
        self.protocol = proxy.get('protocol')
        self.proxy = "%s://%s" % (proxy.get('protocol'), proxy.get('location'))

        for platform in self.config["platform"]:
            self.platforms[platform] = Platform(platform, configs, self)

    async def init(self):
        psq_con = self.connections.get("postgres")
        self.postgres = await asyncpg.create_pool(
            host=psq_con.get("hostname"),
            port=psq_con.get("port"),
            user=psq_con.get("user"),
            database=psq_con.get("database"),
            password=os.getenv(psq_con.get("password_env")),
        )

    async def init_shutdown(self, *args, **kwargs):
        """Shutdown handler"""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

    async def handle_shutdown(self):
        """Close db connection pool after services have shut down."""
        await self.postgres.close()

    async def run(self):
        """Run."""
        await self.init()
        await asyncio.gather(*[
            asyncio.create_task(platform.run()) for platform in self.platforms.values()
        ])
        await self.handle_shutdown()
