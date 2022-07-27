"""Summoner ID Updater Module."""
import asyncio
import logging
import os

from lightshield.services.match_details.service import Platform
import aio_pika
from lightshield.config import Config


class Handler:
    is_shutdown = False
    postgres = None
    platforms = {}
    db = None

    def __init__(self):
        self.config = Config()
        self.logging = logging.getLogger("Handler")
        self.protocol = self.config.proxy.protocol
        self.proxy = self.config.proxy.string

        self.service = self.config.services.match_details
        self.output_folder = self.service.output
        self.logging.info("Output folder: %s", self.output_folder)
        self.semaphores = {}
        for region in self.config.mapping:
            self.semaphores[region] = asyncio.Semaphore(10)

        for region, platforms in self.config.mapping.items():
            for platform in platforms:
                if platform in self.config.active_platforms:
                    self.platforms[platform] = Platform(
                        region, platform, self.config, self, self.semaphores[region]
                    )

    async def init(self):
        self.pika = await aio_pika.connect_robust(
            self.config.rabbitmq._string, loop=asyncio.get_event_loop()
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
