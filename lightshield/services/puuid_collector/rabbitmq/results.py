"""Summoner ID Task Selector."""
import asyncio
import logging
import math
import os
import aio_pika
import asyncpg
import json
from datetime import datetime
import pickle

from lightshield.connection_handler import Connection
from lightshield.services.puuid_collector.rabbitmq import queries
from lightshield.rabbitmq_defaults import QueueHandler


class Handler:
    platforms = {}
    is_shutdown = False
    db = None
    pika = None
    buffered_tasks = {}

    def __init__(self, configs):
        self.logging = logging.getLogger("Task Selector")
        self.config = configs.services.puuid_collector
        self.connection = Connection(config=configs)
        self.platforms = configs.statics.enums.platforms
        self.rabbit = "%s:%s" % (
            configs.connections.rabbitmq.host,
            configs.connections.rabbitmq.port,
        )

    async def init(self):
        self.db = await self.connection.init()
        self.pika = await aio_pika.connect_robust(
            "amqp://user:bitnami@%s/" % self.rabbit, loop=asyncio.get_event_loop()
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

    async def insert_found(self, platform):
        if not self.buffered_tasks[platform]["found"]:
            return
        tasks = self.buffered_tasks[platform]["found"].copy()
        self.buffered_tasks[platform]["found"] = []
        tasks = [pickle.loads(task) for task in tasks]
        self.logging.info(" %s\t | Inserting %s tasks", platform, len(tasks))
        async with self.db.acquire() as connection:
            prep = await connection.prepare(
                queries.update_ranking[self.connection.type].format(
                    platform=platform,
                    platform_lower=platform.lower(),
                    schema=self.connection.schema,
                )
            )
            await prep.executemany([task[:2] for task in tasks])
            converted_results = [
                [
                    res[1],
                    res[2],
                    datetime.fromtimestamp(res[3] / 1000),
                    platform,
                ]
                for res in tasks
            ]
            prep = await connection.prepare(
                queries.insert_summoner[self.connection.type].format(
                    platform=platform,
                    platform_lower=platform.lower(),
                    schema=self.connection.schema,
                )
            )
            await prep.executemany(converted_results)

    async def insert_not_found(self, platform):
        if not self.buffered_tasks[platform]["not_found"]:
            return
        tasks = self.buffered_tasks[platform]["not_found"].copy()
        self.buffered_tasks[platform]["not_found"] = []
        tasks = [task.decode('utf-8') for task in tasks]
        self.logging.info(" %s\t | Inserting %s not found", platform, len(tasks))

        async with self.db.acquire() as connection:
            await connection.execute(
                queries.missing_summoner[self.connection.type].format(
                    platform=platform,
                    platform_lower=platform.lower(),
                    schema=self.connection.schema,
                ),
                tasks,
            )
            del tasks
        for i in range(30):
            await asyncio.sleep(2)
            if self.is_shutdown:
                continue

    async def platform_thread(self, platform):
        self.buffered_tasks[platform] = {"found": [], "not_found": []}
        found_queue = QueueHandler("puuid_results_found_%s" % platform)
        await found_queue.init(durable=True, connection=self.pika)
        not_found_queue = QueueHandler("puuid_results_not_found_%s" % platform)
        await not_found_queue.init(durable=True, connection=self.pika)

        cancel_consume_found = await found_queue.consume_tasks(
            self.process_results, {"platform": platform, "_type": "found"}
        )
        cancel_consume_not_found = await not_found_queue.consume_tasks(
            self.process_results, {"platform": platform, "_type": "not_found"}
        )

        while not self.is_shutdown:

            await self.insert_found(platform)
            await self.insert_not_found(platform)

            for _ in range(30):
                await asyncio.sleep(1)
                if self.is_shutdown:
                    break

        await cancel_consume_found()
        await cancel_consume_not_found()
        await asyncio.sleep(2)
        await self.insert_found(platform)
        await self.insert_not_found(platform)

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
