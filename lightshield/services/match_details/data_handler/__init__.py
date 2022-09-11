"""Summoner ID Task Selector."""
import asyncio
import logging
import aio_pika
import pickle

from lightshield.config import Config
from lightshield.services.match_details import queries
from lightshield.rabbitmq_defaults import QueueHandler
from lightshield.services.match_details.data_handler.parse_tables import (
    generate_player,
    generate_central,
)


class Handler:
    platforms = {}
    is_shutdown = False
    db = None
    pika = None
    buffered_tasks = {}
    # Buffers for messages
    data = []

    def __init__(self):
        self.logging = logging.getLogger("Data Handler")
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

    async def receive_message(self, message):
        """Consumer receiver method for summoner tasks."""
        self.data.append(message)

    async def process_data(self):
        """Process summoners into the database.

        TODO: Add selector to also include new summoners.
        """
        logger = logging.getLogger("Summoners")
        queue = QueueHandler("match_details_data")
        await queue.init(durable=True, prefetch_count=5000, connection=self.pika)
        try:
            while not self.is_shutdown:
                self.data = []
                cancel_consume = await queue.consume_tasks(self.receive_message)
                await asyncio.sleep(5)
                await cancel_consume()
                if not self.data:
                    continue
                tasks = []

                for message in self.data:
                    content = pickle.loads(message.body)
                    info = content["info"]
                    central = await generate_central(info)
                    bans = {}
                    if "bans" in info["teams"][0]:
                        ban_list = [team["bans"] for team in info["teams"]][0]
                        bans = {
                            entry["pickTurn"]: entry["championId"] for entry in ban_list
                        }
                    for player in info["participants"]:
                        tasks.append(await generate_player(player, bans, central))

                keys = list(tasks[0].keys())

                listed_data = {}
                for entry in tasks:
                    platform = entry["platformId"]
                    if platform not in listed_data:
                        listed_data[platform] = []
                    listed_data[platform].append(tuple(entry.values()))
                for key in listed_data.keys():
                    listed_data[key] = list(set(listed_data[key]))

                async with self.db.acquire() as connection:
                    for key, value in listed_data.items():
                        query = """
                                INSERT INTO data.player_%s (%s)
                                VALUES (%s)
                                ON CONFLICT DO NOTHING                        
                            """ % (
                            key.lower(),
                            ", ".join(['"%s"' % key for key in keys]),
                            ", ".join(
                                ["$%s" % (index + 1) for index in range(len(keys))]
                            ),
                        )
                        prep = await connection.prepare(query)
                        for _ in range(5):
                            try:
                                await prep.executemany(value)
                                break
                            except Exception as err:
                                print(query)
                                print(err)
                for message in self.data:
                    await message.ack()
                logger.info("Consumed %s", len(self.data))
        except Exception as err:
            logger.error(err)

    async def run(self):
        """Run."""
        await self.init()

        await asyncio.gather(asyncio.create_task(self.process_data()))
        await self.handle_shutdown()
