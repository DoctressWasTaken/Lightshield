"""Summoner ID Task Selector."""
import asyncio
import logging

import aio_pika

import pickle

from lightshield.config import Config
from lightshield.services.match_history.rabbitmq import queries
from lightshield.rabbitmq_defaults import QueueHandler


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
        self.platforms = self.config.platforms

    async def init(self):
        self.db = await self.connector.init()
        self.pika = await aio_pika.connect_robust(
            self.config.rabbitmq.string, loop=asyncio.get_event_loop()
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

    async def insert_matches(self, platform):
        if not self.buffered_tasks[platform]["matches"]:
            return
        tasks = self.buffered_tasks[platform]["matches"].copy()
        self.buffered_tasks[platform]["matches"] = []
        tasks = [pickle.loads(task) for task in tasks]
        self.logging.info(" %s\t | %s matches inserted", platform, len(tasks))
        async with self.db.acquire() as connection:

            if len(tasks[0]) == 3:
                prep = await connection.prepare(
                    queries.insert_queue_known[self.config.database].format(
                        schema=self.config.db.schema, platform_lower=platform.lower()
                    )
                )
                await prep.executemany(tasks)
            else:
                prep = await connection.prepare(
                    queries.insert_queue_known[self.config.database].format(
                        schema=self.config.db.schema, platform_lower=platform.lower()
                    )
                )
                await prep.executemany(tasks)

    async def insert_summoners(self, platform):
        if not self.buffered_tasks[platform]["summoners"]:
            return
        tasks = self.buffered_tasks[platform]["summoners"].copy()
        self.buffered_tasks[platform]["summoners"] = []
        tasks = [pickle.loads(task) for task in tasks]

        self.logging.info(" %s\t | Updating %s summoners", platform, len(tasks))

        async with self.db.acquire() as connection:
            prep = await connection.prepare(
                queries.update_players[self.config.database].format(
                    schema=self.config.db.schema,
                )
            )
            await prep.executemany(tasks)

    async def platform_thread(self, platform):
        try:
            matches_queue = QueueHandler("match_history_results_matches_%s" % platform)
            await matches_queue.init(durable=True, connection=self.pika)

            summoners_queue = QueueHandler(
                "match_history_results_summoners_%s" % platform
            )
            await summoners_queue.init(durable=True, connection=self.pika)

            self.buffered_tasks[platform] = {"matches": [], "summoners": []}

            cancel_consume_matches = await matches_queue.consume_tasks(
                self.process_results, {"platform": platform, "_type": "matches"}
            )
            cancel_consume_summoners = await summoners_queue.consume_tasks(
                self.process_results, {"platform": platform, "_type": "summoners"}
            )

            while not self.is_shutdown:

                await self.insert_matches(platform)
                await self.insert_summoners(platform)

                for _ in range(30):
                    await asyncio.sleep(1)
                    if self.is_shutdown:
                        break

            await cancel_consume_matches()
            await cancel_consume_summoners()
            await asyncio.sleep(2)
            await self.insert_matches(platform)
            await self.insert_summoners(platform)
        except Exception as err:
            self.logging.info(err)

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
