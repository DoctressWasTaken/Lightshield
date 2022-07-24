"""Summoner ID Updater Module."""
import asyncio
import logging

import aio_pika

from lightshield.services.puuid_collector.service import Platform
from lightshield.config import Config


class Handler:
    is_shutdown = False
    pika = None

    def __init__(self):
        self.logging = logging.getLogger("Handler")
        self.config = Config()
        self.protocol = self.config.proxy.protocol
        self.proxy = self.config.proxy.string
        self.platforms = {
            platform: Platform(platform, self)
            for platform in self.config.active_platforms}

    async def init(self):
        self.pika = await aio_pika.connect_robust(
            self.config.rabbitmq.string, loop=asyncio.get_event_loop()
        )

    async def init_shutdown(self, *args, **kwargs):
        """Shutdown handler"""
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
