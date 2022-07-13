"""Summoner ID Updater Module."""
import asyncio
import logging
import os
import socket

import asyncpg

from lightshield.services.puuid_collector.service import Platform
from lightshield.connection_handler import Connection


class Handler:
    platforms = {}
    is_shutdown = False
    db = None

    def __init__(self, configs):
        self.logging = logging.getLogger("Handler")
        self.config = configs.services.puuid_collector
        self.connection = Connection(config=configs)
        self.protocol = configs.connections.proxy.protocol
        self.proxy = "%s://%s" % (
            configs.connections.proxy.protocol,
            configs.connections.proxy.location
        )

        for platform in configs.statics.enums.platforms:
            self.platforms[platform] = Platform(platform, self)

    async def init(self):
        self.db = await self.connection.init()

    async def init_shutdown(self, *args, **kwargs):
        """Shutdown handler"""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

    async def handle_shutdown(self):
        """Close db connection pool after services have shut down."""
        await self.db.close()

    async def run(self):
        """Run."""
        await self.init()
        await asyncio.gather(
            *[
                asyncio.create_task(platform.run())
                for platform in self.platforms.values()
            ]
        )
        await self.handle_shutdown()
