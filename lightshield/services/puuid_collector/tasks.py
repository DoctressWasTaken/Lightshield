"""Summoner ID Task Selector."""
import asyncio
import logging
import aio_pika
import asyncpg

from lightshield.services.puuid_collector import queries
from lightshield.rabbitmq_defaults import QueueHandler
from lightshield.config import Config
from lightshield.services import HandlerTemplate
from lightshield.services import fail_loop


class Handler(HandlerTemplate):
    pika = None
    buffered_tasks = {}
    handlers = []

    def __init__(self):
        super().__init__()

        self.platforms = self.config.active_platforms
        for platform in self.platforms:
            self.buffered_tasks[platform] = {}

    async def gather_tasks(self, platform, count):
        """Get tasks from db."""
        while not self.is_shutdown:
            async with self.db.acquire() as connection:
                query = queries.tasks.format(
                    platform=platform,
                    platform_lower=platform.lower(),
                )
                try:
                    return await fail_loop(
                        connection.fetch, [query, count], self.logging
                    )

                except asyncpg.InternalServerError:
                    self.logging.info("Internal server error with db.")
            await asyncio.sleep(1)

    async def platform_handler(self, platform):
        self.logging.info("Worker %s up.", platform)
        expected_size = 12000
        # setup
        handler = QueueHandler("puuid_tasks_%s" % platform)
        self.handlers.append(handler)
        await handler.init(durable=True, connection=self.pika)

        while not self.is_shutdown:

            actual_count = await handler.wait_threshold(int(0.75 * expected_size))

            tasks = await self.gather_tasks(
                platform=platform, count=expected_size - actual_count + 500
            )
            if tasks:
                task_list = [
                    [task["summoner_id"], task["summoner_id"].encode()]
                    for task in tasks
                ]
                await handler.send_tasks(task_list, persistent=True, deduplicate=True)
            else:
                await asyncio.sleep(10)

            for _ in range(10):
                await asyncio.sleep(2)
                if self.is_shutdown:
                    break

    async def run(self):
        """Run."""
        await self.init_db()
        await self.init_rabbitmq()

        await asyncio.gather(
            *[
                asyncio.create_task(self.platform_handler(platform))
                for platform in self.platforms
            ]
        )
        await self.cleanup()
