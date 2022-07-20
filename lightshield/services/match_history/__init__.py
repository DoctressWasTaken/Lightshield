"""Match History"""
import asyncio
import logging
import os

from lightshield.services.match_history.service import Platform

import aio_pika


class Handler:
    platforms = {}
    is_shutdown = False
    db = None

    def __init__(self, configs):
        self.logging = logging.getLogger("Service")
        self.service = configs.services.match_history
        self.configs = configs
        self.protocol = configs.connections.proxy.protocol
        self.proxy = "%s://%s" % (
            configs.connections.proxy.protocol,
            configs.connections.proxy.location,
        )
        self.rabbit = "%s:%s" % (
            configs.connections.rabbitmq.host,
            configs.connections.rabbitmq.port,
        )

    async def init(self):
        self.pika = await aio_pika.connect_robust(
            "amqp://user:bitnami@%s/" % self.rabbit, loop=asyncio.get_event_loop()
        )
        for region, platforms in self.configs.statics.mapping.__dict__.items():
            region_semaphore = asyncio.Semaphore(10)
            for platform in platforms:
                self.platforms[platform] = Platform(
                    region, platform, self.configs, self, region_semaphore
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
