"""League Updater Module."""
import asyncio
import logging
import os

import asyncpg

from lightshield.services.league_ranking.service import Service



class Handler:
    is_shutdown = False
    platforms = {}
    redis = postgres = None

    def __init__(self, config):
        self.logging = logging.getLogger("Handler")
        self.connections = config.connections
        self.service = config.services.league_ranking
        proxy = self.connections.proxy

        self.protocol = proxy.protocol
        self.proxy = "%s://%s" % (proxy.protocol, proxy.location)

        select_platforms = self.service.platforms or config.statics.enums.platforms
        for platform in select_platforms:
            self.platforms[platform] = Service(platform, config, self)

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
        """Initiate shutdown."""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

    async def handle_shutdown(self):
        await self.postgres.close()

    async def run(self):
        """Run."""
        await self.init()
        tasks = []
        for platform in self.platforms.values():
            tasks.append(asyncio.create_task(platform.run()))

        await asyncio.gather(*tasks)
        await self.handle_shutdown()
