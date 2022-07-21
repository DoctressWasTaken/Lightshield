"""Match History"""
import asyncio
import logging

from lightshield.services.match_history.service import Platform
from lightshield.config import Config

import aio_pika


class Handler:
    platforms = {}
    is_shutdown = False
    db = None

    def __init__(self):
        self.config = Config()
        self.logging = logging.getLogger("Handler")
        self.protocol = self.config.proxy.protocol
        self.proxy = self.config.proxy.string

    async def init(self):
        self.pika = await aio_pika.connect_robust(
            self.config.rabbitmq.string, loop=asyncio.get_event_loop()
        )
        for region, platforms in self.config.mapping.items():
            region_semaphore = asyncio.Semaphore(10)
            for platform in platforms:
                self.platforms[platform] = Platform(
                    region, platform, self.config, self, region_semaphore
                )

    async def init_shutdown(self, *args, **kwargs):
        """Initiate shutdown."""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

    async def handle_shutdown(self):
        await self.pika.close()

    async def run(self):
        """Run."""
        await self.init()
        tasks = []
        for platform in self.platforms.values():
            tasks.append(asyncio.create_task(platform.run()))

        await asyncio.gather(*tasks)
        await self.handle_shutdown()
