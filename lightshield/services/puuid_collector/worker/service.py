import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp
import aio_pika
import pickle
from lightshield.services.puuid_collector import queries


class Platform:
    _runner = None
    channel = None

    def __init__(self, platform, handler):
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger("%s" % platform)
        # Output queues
        self.results = asyncio.Queue()
        self.not_found = asyncio.Queue()

        self.retry_after = datetime.now()
        self.endpoint_url = f"{handler.protocol}://{platform.lower()}.api.riotgames.com/lol/summoner/v4/summoners/%s"
        self.parallel = 25
        conn = aiohttp.TCPConnector(limit=0)
        self.session = aiohttp.ClientSession(connector=conn)
        self.active = 0

    async def run(self):
        """Main object loop."""
        self.channel = await self.handler.pika.channel()
        await self.channel.set_qos(prefetch_count=self.parallel)
        task_queue = await self.channel.declare_queue(
            "puuid_tasks_%s" % self.platform, durable=True, passive=True,
            arguments={'x-message-deduplication': True}
        )
        await self.channel.declare_queue(
            "puuid_results_%s" % self.platform, durable=True,
        )

        inserter = asyncio.create_task(self.insert_updates())
        consumer = await task_queue.consume(self.task_handler)
        last_prefetch = self.parallel
        while not self.handler.is_shutdown:
            if last_prefetch != self.parallel:
                await self.channel.set_qos(prefetch_count=self.parallel)
                last_prefetch = self.parallel
            await asyncio.sleep(1)
        await task_queue.cancel(consumer)
        while self.active > 0:
            await asyncio.sleep(0.1)
        await inserter

    async def insert_updates(self):
        """Batch insert updates into postgres and marks the rabbitmq messages as completed."""

        while True:
            if self.results.qsize() + self.not_found.qsize() >= 50 or self.handler.is_shutdown:
                found = []
                while True:
                    try:
                        found.append(self.results.get_nowait())
                        self.results.task_done()
                    except:
                        break
                not_found = []
                while True:
                    try:
                        not_found.append(self.not_found.get_nowait())
                        self.results.task_done()
                    except:
                        break
                async with self.handler.db.acquire() as connection:
                    async with connection.transaction():
                        prep = await connection.prepare(
                            queries.update_ranking.format(
                                platform=self.platform,
                                platform_lower=self.platform.lower(),
                            )
                        )
                        await prep.executemany([task['data'] for task in found])
                        for task in found:
                            await task['message'].ack()

                        await connection.execute(
                            queries.missing_summoner.format(
                                platform=self.platform,
                                platform_lower=self.platform.lower(),
                            ),
                            [task['data'] for task in not_found],
                        )
                        for task in not_found:
                            await task['message'].ack()
            if self.handler.is_shutdown:
                break
            await asyncio.sleep(2)

    async def task_handler(self, message):
        """Execute requests."""
        self.active += 1
        seconds = (self.retry_after - datetime.now()).total_seconds()
        if seconds >= 0.1:
            await asyncio.sleep(seconds)
        async with message.process(ignore_processed=True):
            account_id = message.body.decode("utf-8")
            url = self.endpoint_url % account_id
            try:
                async with self.session.get(url, proxy=self.handler.proxy) as response:
                    data = await response.json()
                match response.status:
                    case 200:
                        # Summoner insert goes into the next rabbitmq queue
                        await self.channel.default_exchange.publish(
                            aio_pika.Message(
                                pickle.dumps(
                                    (
                                        data["puuid"],
                                        data["name"],
                                        data["revisionDate"],
                                    )
                                ),
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key="puuid_results_found_%s" % self.platform,
                        )
                        # Ranking update goes into the internal queue
                        await self.results.put({
                            'data': [data['id'], data['puuid']],
                            'message': message
                        })
                        self.logging.debug("200 | %s", url)
                    case 404:
                        await self.not_found.put({'data': account_id, 'message': message})
                        self.logging.debug("404 | %s", url)
                    case 429:
                        await message.reject(requeue=True)
                        self.retry_after = datetime.now() + timedelta(seconds=0.5)
                    case 430:
                        await message.reject(requeue=True)
                        self.retry_after = datetime.fromtimestamp(data["Retry-At"])
                    case _:
                        await message.reject(requeue=True)
            except aiohttp.ContentTypeError:
                await message.reject(requeue=True)
                raise
                # self.logging.error("Response was not a json.")
            except aiohttp.ClientProxyConnectionError:
                await message.reject(requeue=True)
                pass
            except aiohttp.ClientOSError:
                await message.reject(requeue=True)
                raise
                # self.logging.error("Connection reset.")
            except Exception as err:
                self.logging.error(err)
            finally:
                self.active -= 1
