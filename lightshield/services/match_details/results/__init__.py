"""Summoner ID Task Selector."""
import asyncio
import logging
import aio_pika
import pickle

from lightshield.config import Config
from lightshield.services.match_details.rabbitmq import queries
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

    async def change_summoners(self, platform):
        if not self.buffered_tasks[platform]["summoners"]:
            return
        raw_tasks = self.buffered_tasks[platform]["summoners"].copy()
        self.buffered_tasks[platform]["summoners"] = []
        tasks = []
        for package in [pickle.loads(task) for task in raw_tasks]:
            tasks += package
        async with self.db.acquire() as connection:
            prep = await connection.prepare(queries.summoners_update_only)
            await prep.executemany(tasks)
        self.logging.debug(" %s\t | Updating %s summoners", platform, len(tasks))

    async def platform_thread(self, platform):
        try:
            summoner_queue = QueueHandler("match_details_results_%s" % platform)
            await summoner_queue.init(durable=True, connection=self.pika)

            self.buffered_tasks[platform] = {
                "summoners": [],
            }

            cancel_consume_summoners = await summoner_queue.consume_tasks(
                self.process_results, {"platform": platform, "_type": "summoners"}
            )

            while not self.is_shutdown:

                await self.change_summoners(platform)

                for _ in range(30):
                    await asyncio.sleep(1)
                    if self.is_shutdown:
                        break

            await cancel_consume_summoners()
            await asyncio.sleep(4)
            await self.change_summoners(platform)
        except Exception as err:
            self.logging.error(err)

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
