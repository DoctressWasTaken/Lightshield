"""Summoner ID Task Selector."""
import asyncio
import logging
import aio_pika
from datetime import datetime
import pickle

from lightshield.config import Config
from lightshield.services.puuid_collector import queries
from lightshield.rabbitmq_defaults import QueueHandler
from lightshield.services import fail_loop


class Handler:
    platforms = {}
    is_shutdown = False
    db = None
    pika = None
    buffered_tasks = {}

    def __init__(self):
        self.logging = logging.getLogger("Task Selector")
        self.config = Config()
        self.connector = self.config.get_db_connection()
        self.platforms = self.config.active_platforms

    async def init(self):
        self.db = await self.connector.init()
        self.pika = await aio_pika.connect_robust(
            self.config.rabbitmq._string, loop=asyncio.get_event_loop()
        )

    async def init_shutdown(self, *args, **kwargs):
        """Shutdown handler"""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

    async def handle_shutdown(self):
        """Close db connection pool after services have shut down."""
        await self.db.close()
        await self.pika.close()

    async def process_results(self, message, platform, _type):
        """Put results from queue into list."""
        async with message.process(ignore_processed=True):
            self.buffered_tasks[platform][_type].append(message.body)
            await message.ack()

    async def insert_summoners(self, platform):
        if not self.buffered_tasks[platform]["found"]:
            return
        tasks = self.buffered_tasks[platform]["found"].copy()
        self.buffered_tasks[platform]["found"] = []
        tasks = [pickle.loads(task) for task in tasks]
        self.logging.info(" %s\t | Inserting %s tasks", platform, len(tasks))
        async with self.db.acquire() as connection:
            converted_results = [
                [
                    task[0],
                    task[1],
                    datetime.fromtimestamp(task[2] / 1000),
                    platform,
                ]
                for task in tasks
            ]
            prep = await connection.prepare(
                queries.insert_summoner.format(
                    platform=platform,
                    platform_lower=platform.lower(),
                )
            )
            await fail_loop(prep.executemany, [converted_results], self.logging)

    async def platform_thread(self, platform):
        self.buffered_tasks[platform] = {"found": []}
        summoners_queue = QueueHandler("puuid_results_%s" % platform)
        await summoners_queue.init(
            durable=True, prefetch_count=100, connection=self.pika
        )
        cancel_consume_found = await summoners_queue.consume_tasks(
            self.process_results, {"platform": platform, "_type": "found"}
        )

        while not self.is_shutdown:

            await self.insert_summoners(platform)

            for _ in range(30):
                await asyncio.sleep(1)
                if self.is_shutdown:
                    break

        await cancel_consume_found()
        await asyncio.sleep(2)
        await self.insert_summoners(platform)

    async def run(self):
        """Run."""
        await self.init()

        await asyncio.gather(
            *[
                asyncio.create_task(self.platform_thread(platform=platform))
                for platform in self.platforms
            ]
        )

        await self.handle_shutdown()
