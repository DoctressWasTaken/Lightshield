"""Summoner ID Updater Module."""
import asyncio
import logging
import os
import asyncpg

from lightshield.services.match_details.service import Platform


class Handler:
    is_shutdown = False
    postgres = None
    platforms = {}

    def __init__(self, configs):
        self.logging = logging.getLogger("Service")
        # Buffer
        self.connections = configs.connections
        proxy = self.connections.proxy
        self.proxy = "%s://%s" % (proxy.protocol, proxy.location)
        self.service = configs.services.match_history
        self.configs = configs

    async def init(self):
        psq_con = self.connections.postgres
        self.postgres = await asyncpg.create_pool(
            host=psq_con.hostname,
            port=psq_con.port,
            user=psq_con.user,
            database=psq_con.database,
            password=os.getenv(psq_con.password_env),
        )

        for region, platforms in self.configs.statics.mapping.__dict__.items():
            for platform in platforms:
                self.platforms[platform] = Platform(
                    region, platform, self.configs, self
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
