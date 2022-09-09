"""Match History"""
import asyncio
import logging

from lightshield.services.match_history.worker.service import Platform
from lightshield.config import Config
from lightshield.rabbitmq_defaults import QueueHandler

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
        for region, platforms in self.config.mapping.items():
            region_semaphore = asyncio.Semaphore(10)
            for platform in platforms:
                if platform in self.config.active_platforms:
                    self.platforms[platform] = Platform(
                        region, platform, self.config, self, region_semaphore
                    )
        self.connector = self.config.get_db_connection()

    async def init(self):
        self.pika = await aio_pika.connect_robust(
            self.config.rabbitmq._string, loop=asyncio.get_event_loop()
        )
        self.db = await self.connector.init()

    async def init_shutdown(self, *args, **kwargs):
        """Initiate shutdown."""

        await asyncio.gather(*[asyncio.create_task(platform.shutdown()) for platform in self.platforms.values()])
        self.is_shutdown = True

    async def handle_shutdown(self):
        await self.pika.close()

    async def run(self):
        """Run."""
        await self.init()
        results_queue = QueueHandler("match_history_results")
        await results_queue.init(durable=True, connection=self.pika)
        tasks = []
        for platform in self.platforms.values():
            tasks.append(asyncio.create_task(platform.run(results_queue)))

        await asyncio.gather(*tasks)
        await self.handle_shutdown()
