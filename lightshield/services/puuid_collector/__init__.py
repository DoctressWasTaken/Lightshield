"""Summoner ID Updater Module."""
import asyncio
import logging
import os

import asyncpg
import uvloop

uvloop.install()

from lightshield.services.puuid_collector.service import Platform


class Handler:
    platforms = {}
    is_shutdown = False
    postgres = None

    def __init__(self, configs):
        self.logging = logging.getLogger("Handler")
        self.connections = configs.connections
        self.config = configs.services.puuid_collector
        proxy = self.connections.proxy
        self.protocol = proxy.protocol
        self.proxy = "%s://%s" % (proxy.protocol, proxy.location)

        for platform in configs.statics.enums.platforms:
            self.platforms[platform] = Platform(platform, self)

    async def init(self):
        psq_con = self.connections.postgres
        self.postgres = await asyncpg.create_pool(
            host=psq_con.hostname,
            port=psq_con.port,
            user=psq_con.user,
            database=psq_con.database,
            password=os.getenv(psq_con.password_env),
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
