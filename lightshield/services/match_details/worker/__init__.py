"""Summoner ID Updater Module."""
import asyncio
import logging
import os
import types

from lightshield.services.match_details.worker.service import Platform
import aio_pika
from lightshield.config import Config
from lightshield.rabbitmq_defaults import QueueHandler


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
        self.semaphores = {}
        for region in self.config.mapping:
            self.semaphores[region] = asyncio.Semaphore(10)

        for region, platforms in self.config.mapping.items():
            for platform in platforms:
                if platform in self.config.active_platforms:
                    self.platforms[platform] = Platform(
                        region, platform, self.config, self, self.semaphores[region]
                    )
        self.connector = self.config.get_db_connection()

    async def init(self):
        self.pika = await aio_pika.connect_robust(
            self.config.rabbitmq._string, loop=asyncio.get_event_loop()
        )
        self.db = await self.connector.init()

    async def shutdown(self, *args, **kwargs):
        """Initiate shutdown."""
        self.logging.info("Received shutdown signal.")
        await asyncio.gather(
            *[
                asyncio.create_task(platform.shutdown())
                for platform in self.platforms.values()
            ]
        )
        self.is_shutdown = True

    async def cleanup(self):
        await self.pika.close()

    async def run(self):
        """Run."""
        await self.init()
        output_queues = types.SimpleNamespace()
        output_queues.match = QueueHandler("match_details_results")
        await output_queues.match.init(durable=True, connection=self.pika)

        output_queues.data = QueueHandler("match_details_data")
        await output_queues.data.init(durable=True, connection=self.pika)

        output_queues.summoners = QueueHandler("match_details_summoner_updates")
        await output_queues.summoners.init(durable=True, connection=self.pika)

        tasks = []
        for platform in self.platforms.values():
            tasks.append(asyncio.create_task(platform.run(output_queues)))

        await asyncio.gather(*tasks)
        await self.cleanup()
