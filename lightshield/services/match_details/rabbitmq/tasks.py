"""Match History Task Selector."""
import asyncio
import logging
import math
import aio_pika
import asyncpg
import pickle
from datetime import datetime, timedelta

from lightshield.connection_handler import Connection
from lightshield.services.match_details.rabbitmq import queries
from lightshield.rabbitmq_defaults import QueueHandler


class Handler:
    platforms = {}
    is_shutdown = False
    db = None
    pika = None
    buffered_tasks = {}
    handlers = []

    def __init__(self, configs):
        self.logging = logging.getLogger("Task Selector")
        self.config = configs.services.puuid_collector
        self.connection = Connection(config=configs)
        self.platforms = configs.statics.enums.platforms
        for platform in self.platforms:
            self.buffered_tasks[platform] = {}
        self.rabbit = "%s:%s" % (
            configs.connections.rabbitmq.host,
            configs.connections.rabbitmq.port,
        )
        self.service = configs.services.match_history

    async def init(self):
        self.db = await self.connection.init()
        self.pika = await aio_pika.connect_robust(
            "amqp://user:bitnami@%s/" % self.rabbit, loop=asyncio.get_event_loop()
        )

    async def init_shutdown(self, *args, **kwargs):
        """Shutdown handler"""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True
        for handler in self.handlers:
            handler.shutdown()

    async def handle_shutdown(self):
        """Close db connection pool after services have shut down."""
        await self.db.close()
        await self.pika.close()

    async def gather_tasks(self, platform, count):
        """Get tasks from db."""
        while not self.is_shutdown:
            async with self.db.acquire() as connection:
                try:
                    return await connection.fetch(
                        queries.tasks[self.connection.type].format(
                            platform=platform,
                            platform_lower=platform.lower(),
                            schema=self.connection.schema,
                        ),
                        count,
                    )
                except asyncpg.InternalServerError:
                    self.logging.info("Internal server error with db.")
            await asyncio.sleep(1)

    async def platform_handler(self, platform):
        # setup
        sections = 8
        section_size = 1000
        task_backlog = []
        handler = QueueHandler("match_details_tasks_%s" % platform)
        self.handlers.append(handler)
        await handler.init(durable=True, connection=self.pika)

        await handler.wait_threshold(0)

        while not self.is_shutdown:
            remaining_sections = math.ceil(
                (await handler.wait_threshold((sections - 1) * section_size)) / 1000
            )
            # Drop used up sections
            task_backlog = task_backlog[-remaining_sections * section_size :]
            # Get tasks
            while not self.is_shutdown:
                tasks = await self.gather_tasks(
                    platform=platform, count=sections * section_size
                )
                to_add = []
                for task in tasks:
                    matchId = task["match_id"]
                    if matchId not in task_backlog:
                        task_backlog.append(matchId)
                        to_add.append(str(matchId).encode())
                    if len(task_backlog) >= sections * section_size:
                        break
                if not to_add:
                    await asyncio.sleep(10)
                    continue
                await handler.send_tasks(to_add, persistent=True)
                break

    async def run(self):
        """Run."""
        await self.init()
        await asyncio.gather(
            *[
                asyncio.create_task(self.platform_handler(platform))
                for platform in self.platforms
            ]
        )
        await self.handle_shutdown()
