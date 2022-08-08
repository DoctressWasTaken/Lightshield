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

    async def update_matches_200(self, platform):
        if not self.buffered_tasks[platform]["matches_200"]:
            return
        tasks = self.buffered_tasks[platform]["matches_200"].copy()
        self.buffered_tasks[platform]["matches_200"] = []
        tasks = [pickle.loads(task) for task in tasks]
        async with self.db.acquire() as connection:
            prep = await connection.prepare(
                queries.flush_found.format(
                    platform_lower=platform.lower(),
                    platform=platform,
                )
            )
            await prep.executemany(tasks)
        self.logging.info(" %s\t | %s found matches inserted", platform, len(tasks))

    async def update_matches_404(self, platform):
        if not self.buffered_tasks[platform]["matches_404"]:
            return
        tasks = self.buffered_tasks[platform]["matches_404"].copy()
        self.buffered_tasks[platform]["matches_404"] = []
        tasks = [[int(task.decode("utf-8"))] for task in tasks]
        async with self.db.acquire() as connection:
            prep = await connection.prepare(
                queries.flush_missing.format(
                    platform_lower=platform.lower(),
                    platform=platform,
                )
            )
            await prep.executemany(tasks)
        self.logging.info(" %s\t | %s missing matches inserted", platform, len(tasks))

    async def change_summoners(self, platform):
        if not self.buffered_tasks[platform]["summoners"]:
            return
        raw_tasks = self.buffered_tasks[platform]["summoners"].copy()
        self.buffered_tasks[platform]["summoners"] = []
        tasks = []
        for package in [pickle.loads(task) for task in raw_tasks]:
            tasks += package
        async with self.db.acquire() as connection:
            prep = await connection.prepare(
                queries.summoners_update_only
            )
            await prep.executemany(tasks)
        self.logging.info(" %s\t | Updating %s summoners", platform, len(tasks))

    async def platform_thread(self, platform):
        try:
            matches_queue_200 = QueueHandler(
                "match_details_results_matches_200_%s" % platform
            )
            await matches_queue_200.init(durable=True, connection=self.pika)

            matches_queue_404 = QueueHandler(
                "match_details_results_matches_404_%s" % platform
            )
            await matches_queue_404.init(durable=True, connection=self.pika)

            summoner_queue = QueueHandler(
                "match_details_results_summoners_%s" % platform
            )
            await summoner_queue.init(durable=True, connection=self.pika)

            self.buffered_tasks[platform] = {
                "matches_200": [],
                "matches_404": [],
                "summoners": [],
            }

            cancel_consume_matches_200 = await matches_queue_200.consume_tasks(
                self.process_results, {"platform": platform, "_type": "matches_200"}
            )
            cancel_consume_matches_404 = await matches_queue_404.consume_tasks(
                self.process_results, {"platform": platform, "_type": "matches_404"}
            )
            cancel_consume_summoners = await summoner_queue.consume_tasks(
                self.process_results, {"platform": platform, "_type": "summoners"}
            )

            while not self.is_shutdown:

                await self.update_matches_200(platform)
                await self.update_matches_404(platform)
                await self.change_summoners(platform)

                for _ in range(30):
                    await asyncio.sleep(1)
                    if self.is_shutdown:
                        break

            await cancel_consume_matches_200()
            await cancel_consume_matches_404()
            await cancel_consume_summoners()
            await asyncio.sleep(4)
            await self.update_matches_200(platform)
            await self.update_matches_404(platform)
            await self.change_summoners(platform)
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
