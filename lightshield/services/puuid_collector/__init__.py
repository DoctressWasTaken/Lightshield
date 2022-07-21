"""Summoner ID Updater Module."""
import asyncio
import logging

import aio_pika

from lightshield.services.puuid_collector.service import Platform
from lightshield.config import Config


class Handler:
    platforms = {}
    is_shutdown = False
    pika = None

    def __init__(self):
        self.logging = logging.getLogger("Handler")
        self.config = Config()
        self.protocol = self.config.proxy.protocol
        self.proxy = self.config.proxy.string
        for platform in [platform for platform, conf in self.config.platforms.items() if not conf['disabled']]:
            self.platforms[platform] = Platform(platform, self)

    async def init(self):
        self.pika = await aio_pika.connect_robust(self.config.rabbitmq.string, loop=asyncio.get_event_loop())

    async def init_shutdown(self, *args, **kwargs):
        """Shutdown handler"""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

    async def handle_shutdown(self):
        """Close db connection pool after services have shut down."""
        await self.pika.close()

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
