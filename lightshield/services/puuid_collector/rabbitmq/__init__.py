"""Summoner ID Task Selector."""
import asyncio
import logging
import math
import os
import aio_pika
import asyncpg
import json
from datetime import datetime

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

    async def gather_tasks(self, platform, count):
        """Get tasks from db."""
        while not self.is_shutdown:
            async with self.db.acquire() as connection:
                query = queries.tasks[self.connection.type].format(
                    platform=platform,
                    platform_lower=platform.lower(),
                    schema=self.connection.schema
                )
                try:
                    return await connection.fetch(query, count)

                except asyncpg.InternalServerError:
                    self.logging.info("Internal server error with db.")
            await asyncio.sleep(1)

    async def process_results(self, platform, base_threshold):
        queue = 'puuid_results_found_%s' % platform
        channel = await self.pika.channel()
        await channel.set_qos(prefetch_count=base_threshold)
        await channel.declare_queue(queue, durable=True)

        threshold = base_threshold
        while not self.is_shutdown:
            res = await channel.declare_queue(
                queue, durable=True, passive=True)
            if res.declaration_result.message_count < threshold:
                threshold = max(1, threshold - 1)
                await asyncio.sleep(2)
                continue
            self.logging.info("Found %s tasks to insert" % res.declaration_result.message_count)
            tasks = []
            async with res.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process(ignore_processed=True):
                        threshold -= 1
                        msg = message.body.decode('utf-8')
                        tasks.append(json.loads(msg))
                        await message.ack()
                    if threshold <= 0:
                        break
            threshold = base_threshold
            async with self.db.acquire() as connection:
                await connection.execute(
                    queries.update_ranking[self.connection.type].format(
                        platform=platform,
                        platform_lower=platform.lower(),
                        schema=self.connection.schema
                    ) % ",".join(
                        ["('%s', '%s', '%s')" % (res[0], platform, res[1]) for res in tasks]
                    ))
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

    async def platform_handler(self, platform):
        # setup
        processors = [
            asyncio.create_task(self.process_results(platform, 500))
        ]
        self.logging.info("Starting handler for %s. Waiting for empty queue.", platform)
        sections = 8
        section_size = 1000
        task_backlog = []
        async with self.pika:
            channel = await self.pika.channel()
            await channel.declare_queue(
                'puuid_tasks_%s' % platform, durable=True)
            # Wait till the queue is empty before starting (To avoid duplicates)
            while (await channel.declare_queue('puuid_tasks_%s' % platform, passive=True
                                               )).declaration_result.message_count > 0:
                if self.is_shutdown:
                    return
                await asyncio.sleep(2)

            while not self.is_shutdown:
                await asyncio.sleep(2)

                queue_size = (await channel.declare_queue('puuid_tasks_%s' % platform, passive=True
                                                          )).declaration_result.message_count

                if (sections_remaining := math.ceil(queue_size / section_size)) < sections:
                    # Drop used up sections
                    task_backlog = task_backlog[-sections_remaining * section_size:]
                    # Try to get new tasks
                    tasks = await self.gather_tasks(platform=platform, count=sections * section_size)
                    # Exit if no tasks
                    if not tasks:
                        await asyncio.sleep(10)
                        continue
                    self.logging.info("%s\t| Queue found to be missing %s sections", platform,
                                      sections - sections_remaining)
                    for task in tasks:
                        if (sId := task['summoner_id']) in task_backlog:
                            continue
                        task_backlog.append(sId)
                        await channel.default_exchange.publish(
                            aio_pika.Message(
                                sId.encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                            ),
                            routing_key='puuid_tasks_%s' % platform
                        )
                        if len(task_backlog) >= sections * section_size:
                            break
        await asyncio.gather(*processors)

    async def run(self):
        """Run."""
        await self.init()
        # tasks = await self.gather_tasks()

        await asyncio.gather(*[
                                  asyncio.create_task(self.platform_handler(platform))
                                  for platform in self.platforms
                              ])

        await self.handle_shutdown()
