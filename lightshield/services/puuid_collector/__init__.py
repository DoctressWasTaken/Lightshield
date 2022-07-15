"""Summoner ID Updater Module."""
import asyncio
import logging

import aio_pika

from lightshield.services.puuid_collector.service import Platform


class Handler:
    platforms = {}
    is_shutdown = False
    pika = None

    def __init__(self, configs):
        self.logging = logging.getLogger("Handler")
        self.config = configs.services.puuid_collector
        self.protocol = configs.connections.proxy.protocol
        self.proxy = "%s://%s" % (
            configs.connections.proxy.protocol,
            configs.connections.proxy.location,
        )
        self.rabbit = "%s:%s" % (
            configs.connections.rabbitmq.host,
            configs.connections.rabbitmq.port,
        )
        for platform in configs.statics.enums.platforms:
            self.platforms[platform] = Platform(platform, self)

    async def init(self):
        self.pika = await aio_pika.connect_robust(
            "amqp://user:bitnami@%s/" % self.rabbit, loop=asyncio.get_event_loop()
        )

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
