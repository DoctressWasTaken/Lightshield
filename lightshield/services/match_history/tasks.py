"""Match History Task Selector."""
import asyncio
import logging
import aio_pika
import asyncpg
import pickle

from lightshield.config import Config
from lightshield.services.match_history import queries
from lightshield.rabbitmq_defaults import QueueHandler


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
                    query = queries.get_tasks.format(
                            found_newer_wait=self.service.min_age.newer_activity,
                            no_activity_wait=self.service.min_age.no_activity,
                            min_activity_age=self.service.min_age.activity_min_age
                        )
                    return await connection.fetch(
                        query,
                        platform,
                        count,
                    )
                except asyncpg.InternalServerError:
                    self.logging.info("Internal server error with db.")
            await asyncio.sleep(1)

    async def platform_handler(self, platform):
        # setup
        expected_size = 12000

        handler = QueueHandler("match_history_tasks_%s" % platform)
        self.handlers.append(handler)
        await handler.init(durable=True, connection=self.pika)

        while not self.is_shutdown:

            actual_count = await handler.wait_threshold(int(0.75 * expected_size))
            self.logging.debug("Refilling %s by %s to %s", platform, expected_size - actual_count + 500, expected_size + 500)
            try:
                tasks = await self.gather_tasks(
                    platform=platform, count=expected_size - actual_count + 500
                )
                if tasks:
                    self.logging.debug("Found %s tasks for %s", len(tasks), platform)
                    task_list = [
                        [
                            task["puuid"],
                            pickle.dumps(
                                (
                                    task["puuid"],
                                    task["latest_match"],
                                    task["last_history_update"],
                                )
                            ),
                        ]
                        for task in tasks
                    ]
                    await handler.send_tasks(task_list, persistent=True)
                else:
                    await asyncio.sleep(10)
            except Exception as err:
                self.logging.error(err)
            for _ in range(10):
                if self.is_shutdown:
                    break
                await asyncio.sleep(2)

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
