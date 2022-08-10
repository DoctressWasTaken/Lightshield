"""Match History Task Selector."""
import asyncio
import logging
import math
import aio_pika
import asyncpg

from lightshield.services.match_details.rabbitmq import queries
from lightshield.rabbitmq_defaults import QueueHandler, Buffer
from lightshield.config import Config


class Handler:
    platforms = {}
    is_shutdown = False
    db = None
    pika = None
    buffered_tasks = {}
    handlers = []

    def __init__(self):
        self.logging = logging.getLogger("Task Selector")
        self.config = Config()
        self.connector = self.config.get_db_connection()
        self.platforms = self.config.active_platforms
        for platform in self.platforms:
            self.buffered_tasks[platform] = {}

    async def init(self):
        self.db = await self.connector.init()
        self.pika = await aio_pika.connect_robust(
            self.config.rabbitmq._string, loop=asyncio.get_event_loop()
        )

    async def init_shutdown(self, *args, **kwargs):
        """Shutdown handler"""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True
        for handler in self.handlers:
            handler.shutdown()

    async def handle_shutdown(self):
        """Close db connection pool after services have shut down."""
        await self.connector.close()
        await self.pika.close()

    async def gather_tasks(self, platform, count):
        """Get tasks from db."""
        while not self.is_shutdown:
            async with self.db.acquire() as connection:
                try:
                    return await connection.fetch(
                        queries.tasks.format(
                            platform=platform,
                            platform_lower=platform.lower(),
                        ),
                        count,
                    )
                except asyncpg.InternalServerError:
                    self.logging.info("Internal server error with db.")
            await asyncio.sleep(1)

    async def platform_handler(self, platform):
        # setup
        buffer = Buffer(platform, block_size=500, blocks=16)

        task_backlog = []
        handler = QueueHandler("match_details_tasks_%s" % platform)
        self.handlers.append(handler)
        await handler.init(durable=True, connection=self.pika)

        await handler.wait_threshold(0)

        while not self.is_shutdown:
            remaining_tasks = await handler.wait_threshold(buffer.get_refill_count())

            if not buffer.needs_refill(remaining_tasks):
                continue

            while not self.is_shutdown:
                tasks = await self.gather_tasks(
                    platform=platform, count=buffer.buffer_size
                )
                # convert to list

                task_list = [task["match_id"] for task in tasks]
                to_add = [str(_id).encode() for _id in buffer.verify_tasks(task_list)]

                if not to_add:
                    await asyncio.sleep(10)
                    continue
                self.logging.info(
                    "%s\t| Refilling queue by %s tasks", platform, len(to_add)
                )
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
