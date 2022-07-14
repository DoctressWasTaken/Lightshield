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
        for platform in self.platforms:
            self.buffered_tasks[platform] = {}
        self.rabbit = "%s:%s" % (
            configs.connections.rabbitmq.host,
            configs.connections.rabbitmq.port
        )

    async def init(self):
        self.db = await self.connection.init()
        self.pika = await aio_pika.connect_robust(
            "amqp://user:bitnami@%s/" % self.rabbit,
            loop=asyncio.get_event_loop()
        )

    async def init_shutdown(self, *args, **kwargs):
        """Shutdown handler"""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

    async def handle_shutdown(self):
        """Close db connection pool after services have shut down."""
        await self.db.close()
        await self.pika.close()

    async def process_results(self, platform):
        queue = 'puuid_results_found_%s' % platform
        channel = await self.pika.channel()
        await channel.set_qos(prefetch_count=500)
        await channel.declare_queue(queue, durable=True)

        while not self.is_shutdown:
            await asyncio.sleep(30)
            res = await channel.declare_queue(
                queue, durable=True, passive=True)
            queue_size = res.declaration_result.message_count
            if queue_size == 0:
                continue
            tasks = []
            for i in range(queue_size):
                try:
                    if task := await queue.get(timeout=5, fail=False):
                        tasks.append(pickle.loads(task.body))
                        await task.ack()
                except Exception as err:
                    self.logging.info("Failed to retrieve task.")
                    pass

            async with self.db.acquire() as connection:
                prep = await connection.prepare(
                    queries.update_ranking[self.connection.type].format(
                        platform=platform,
                        platform_lower=platform.lower(),
                        schema=self.connection.schema
                    ))
                await prep.executemany(tasks)
                converted_results = [
                    [res[1], res[2], datetime.fromtimestamp(res[3] / 1000), platform]
                    for res in tasks
                ]
                prep = await connection.prepare(
                    queries.insert_summoner[self.connection.type].format(
                        platform=platform,
                        platform_lower=platform.lower(),
                        schema=self.connection.schema
                    ))
                await prep.executemany(converted_results)
                self.logging.info(
                    "Updated %s rankings.",
                    len(tasks),
                )
                del tasks

    async def process_not_found(self, platform):
        queue = 'puuid_results_not_found_%s' % platform
        channel = await self.pika.channel()
        await channel.set_qos(prefetch_count=100)
        await channel.declare_queue(queue, durable=True)

        while not self.is_shutdown:
            await asyncio.sleep(30)
            res = await channel.declare_queue(
                queue, durable=True, passive=True)
            queue_size = res.declaration_result.message_count
            if queue_size == 0:
                continue
            tasks = []
            for i in range(queue_size):
                try:
                    if task := await queue.get(timeout=5, fail=False):
                        tasks.append(task.body.decode('utf-8'))
                        await task.ack()
                except Exception as err:
                    pass
            async with self.db.acquire() as connection:
                await connection.execute(
                    queries.missing_summoner[self.connection.type].format(
                        platform=platform,
                        platform_lower=platform.lower(),
                        schema=self.connection.schema
                    ),
                    tasks,
                )
                del tasks

    async def run(self):
        """Run."""
        await self.init()

        tasks = []
        for platform in self.platforms:
            tasks.append(asyncio.create_task(self.process_results(platform)))
            tasks.append(asyncio.create_task(self.process_not_found(platform)))

        await asyncio.gather(*tasks)

        await self.handle_shutdown()
