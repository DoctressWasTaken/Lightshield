"""Match History Task Selector."""
import asyncio
import logging
import math
import aio_pika
import asyncpg
import pickle
from datetime import datetime, timedelta

from lightshield.config import Config
from lightshield.services.match_history.rabbitmq import queries
from lightshield.rabbitmq_defaults import QueueHandler, Buffer


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
        self.service = self.config.services.match_history

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
        await self.db.close()
        await self.pika.close()

    async def gather_tasks(self, platform, count):
        """Get tasks from db."""
        while not self.is_shutdown:
            async with self.db.acquire() as connection:
                try:
                    return await connection.fetch(
                        queries.reserve,
                        platform,
                        self.service.min_age.no_activity,
                        self.service.min_age.newer_activity,
                        count,
                    )
                except asyncpg.InternalServerError:
                    self.logging.info("Internal server error with db.")
            await asyncio.sleep(1)

    async def platform_handler(self, platform):
        # setup
        buffer = Buffer(platform)

        task_backlog = []
        handler = QueueHandler("match_history_tasks_%s" % platform)
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

                task_list = [(
                    task["puuid"],
                    task["latest_match"],
                    task["last_history_update"],
                ) for task in tasks]
                to_add = [pickle.dumps(entry) for entry in buffer.verify_tasks(task_list)]
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
