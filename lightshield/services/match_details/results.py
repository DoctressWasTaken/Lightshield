"""Summoner ID Task Selector."""
import asyncio
import logging
import aio_pika
import pickle

from lightshield.config import Config
from lightshield.services.match_details import queries
from lightshield.rabbitmq_defaults import QueueHandler
from lightshield.services import fail_loop


class Handler:
    platforms = {}
    is_shutdown = False
    db = None
    pika = None
    buffered_tasks = {}
    # Buffers for messages
    summoners = []
    matches = []

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

    async def shutdown(self, *args, **kwargs):
        """Shutdown handler"""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

    async def cleanup(self):
        """Close db connection pool after services have shut down."""
        await self.db.close()
        await self.pika.close()

    async def summoner_message(self, message):
        """Consumer receiver method for summoner tasks."""
        self.summoners.append(message)

    async def match_message(self, message):
        """Consumer receiver method for match tasks."""
        self.matches.append(message)

    async def process_summoners(self):
        """Process summoners into the database.

        TODO: Add selector to also include new summoners.
        """
        logger = logging.getLogger("Summoners")
        queue = QueueHandler("match_details_summoner_updates")
        await queue.init(durable=True, prefetch_count=5000, connection=self.pika)
        try:
            while not self.is_shutdown:
                self.summoners = []
                cancel_consume = await queue.consume_tasks(self.summoner_message)
                await asyncio.sleep(5)
                await cancel_consume()
                if not self.summoners:
                    continue
                tasks = []
                for message in self.summoners:
                    content = pickle.loads(message.body)
                    tasks += [
                        [
                            content["last_activity"],
                            content["platform"],
                            player["name"],
                            player["puuid"],
                        ]
                        for player in content["summoners"]
                    ]
                if tasks:
                    async with self.db.acquire() as connection:
                        prep = await connection.prepare(queries.summoners_update_only)
                        await fail_loop(prep.executemany, [tasks], self.logging)

                for message in self.summoners:
                    await message.ack()
            logger.info("Consumed %s", len(self.summoners))
        except Exception as err:
            logger.error(err)

    async def process_matches(self):
        """Process summoners into the database."""
        logger = logging.getLogger("Matches")
        queue = QueueHandler("match_details_results")
        await queue.init(durable=True, prefetch_count=5000, connection=self.pika)
        try:
            while not self.is_shutdown:
                self.matches = []
                cancel_consume = await queue.consume_tasks(self.match_message)
                await asyncio.sleep(5)
                await cancel_consume()

                if not self.matches:
                    continue
                found = {}
                missing = {}
                for message in self.matches:
                    match = pickle.loads(message.body)
                    platform = match["platform"]
                    if match["found"]:
                        if platform not in found:
                            found[platform] = []
                        found[platform].append(
                            [
                                match["data"]["queue"],
                                match["data"]["creation"],
                                match["data"]["patch"],
                                match["data"]["duration"],
                                match["data"]["win"],
                                match["data"]["matchId"],
                            ]
                        )
                    else:
                        if platform not in missing:
                            missing[platform] = []
                        missing[platform].append([match["data"]["matchId"]])
                async with self.db.acquire() as connection:
                    for platform, values in found.items():
                        if found:
                            prep = await connection.prepare(
                                queries.flush_found.format(
                                    platform_lower=platform.lower(),
                                    platform=platform,
                                )
                            )
                            await fail_loop(prep.executemany, [values], self.logging)

                    for platform, values in missing.items():
                        if missing:
                            await fail_loop(
                                connection.execute,
                                [
                                    queries.flush_missing.format(
                                        platform_lower=platform.lower(),
                                        platform=platform,
                                    ),
                                    values,
                                ],
                                self.logging,
                            )
                for message in self.matches:
                    await message.ack()
                logger.debug("Consumed %s", len(self.matches))
        except Exception as err:
            logger.error(err)

    async def run(self):
        """Run."""
        await self.init()

        await asyncio.gather(
            asyncio.create_task(self.process_matches()),
            asyncio.create_task(self.process_summoners()),
        )

        await self.cleanup()
