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
                        queries.get_tasks,
                        platform,
                        self.service.min_age.newer_activity,
                        self.service.min_age.no_activity,
                        self.service.min_age.activity_min_age,
                        count,
                    )
                except asyncpg.InternalServerError:
                    self.logging.info("Internal server error with db.")
            await asyncio.sleep(1)

    async def platform_handler(self, platform):
        # setup
        buffer = Buffer(platform, block_size=1000, blocks=4)
        expected_size = 4000

        handler = QueueHandler("match_history_tasks_%s" % platform)
        self.handlers.append(handler)
        await handler.init(durable=True, connection=self.pika)

        while not self.is_shutdown:
            async with self.db.acquire() as connection:
                await connection.execute("""
                    DELETE FROM match_history_queue
                    WHERE platform = $1
                    AND added < NOW() - '30 minutes'::INTERVAL
                """, platform)  # Cleanup query for tasks left in the table by accident
                remaining_tasks = await connection.fetchval(
                    """SELECT COUNT(DISTINCT puuid) FROM match_history_queue WHERE platform  = $1 """,
                    platform,
                )
            tasks_to_pull = expected_size - remaining_tasks
            if tasks_to_pull < 500:  # Queue full enough to skip
                await asyncio.sleep(10)
                continue
            self.logging.info(
                "Queue too empty, attempting to find %s new tasks", tasks_to_pull
            )

            task_list = [
                (
                    task["puuid"],
                    task["latest_match"],
                    task["last_history_update"],
                )
                for task in await self.gather_tasks(
                    platform=platform, count=tasks_to_pull
                )
            ]
            to_add = [pickle.dumps(entry) for entry in task_list]
            if not to_add:
                await asyncio.sleep(10)
                continue
            await handler.send_tasks(to_add, persistent=True)

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
