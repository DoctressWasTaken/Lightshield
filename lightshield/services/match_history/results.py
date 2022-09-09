"""Summoner ID Task Selector."""
import asyncio
import logging

import aio_pika

import pickle

from lightshield.config import Config
from lightshield.services.match_history import queries
from lightshield.rabbitmq_defaults import QueueHandler


class Handler:
    is_shutdown = False
    db = None
    pika = None
    messages = []

    def __init__(self):
        self.logging = logging.getLogger("Task Selector")
        self.config = Config()
        self.connector = self.config.get_db_connection()

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

    async def on_message(self, message):
        self.messages.append(message)

    async def run(self):
        """Run."""
        await self.init()
        results_queue = QueueHandler("match_history_results")
        await results_queue.init(durable=True, prefetch_count=5000, connection=self.pika)
        try:
            while not self.is_shutdown:
                self.messages = []
                cancel_consume = await results_queue.consume_tasks(self.on_message)
                await asyncio.sleep(5)  # Collect tasks
                await cancel_consume()

                if not self.messages:
                    continue
                matches = {}
                summoners = []
                for message in self.messages:
                    content = pickle.loads(message.body)
                    for match in content['matches']:
                        if match[0] not in matches:
                            matches[match[0]] = {
                                'no_queue': [],
                                'queue': []
                            }
                        if len(match) == 2:
                            matches[match[0]]['no_queue'].append(match)
                        else:
                            matches[match[0]]['queue'].append(match)
                    summoners.append(content['summoner'])
                async with self.db.acquire() as connection:
                    for platform, matches in matches.items():
                        if matches['queue']:

                            prep = await connection.prepare(
                                queries.insert_queue_known.format(
                                    platform_lower=platform.lower()
                                )
                            )
                            for _ in range(5):
                                try:
                                    await prep.executemany(matches['queue'])
                                    break
                                except Exception as err:
                                    self.logging.error(err)

                        if matches['no_queue']:
                            prep = await connection.prepare(
                                queries.insert_queue_unknown.format(
                                    platform_lower=platform.lower()
                                )
                            )
                            for _ in range(5):
                                try:
                                    await prep.executemany(matches['no_queue'])
                                    break
                                except Exception as err:
                                    self.logging.error(err)
                    prep = await connection.prepare(
                        queries.update_players
                    )
                    await prep.executemany(summoners)
                for message in self.messages:
                    await message.ack()
                self.logging.debug("Consumed %s", len(self.messages))


        except Exception as err:
            self.logging.error(err)
        await self.handle_shutdown()
