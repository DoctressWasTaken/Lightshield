import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp
import aio_pika
import pickle


class Platform:
    _runner = None
    channel = None

    def __init__(self, platform, handler):
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger("%s" % platform)
        # Output queues
        self.results = None
        self.not_found = None

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
            "puuid_results_found_%s" % self.platform, durable=True,
        )
        await self.channel.declare_queue(
            "puuid_results_not_found_%s" % self.platform, durable=True
        )

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

    async def task_handler(self, message):
        """Execute requests."""
        self.active += 1
        seconds = (self.retry_after - datetime.now()).total_seconds()
        if seconds >= 0.1:
            await asyncio.sleep(seconds)
        async with message.process(ignore_processed=True):
            url = self.endpoint_url % message.body.decode("utf-8")
            try:
                async with self.session.get(url, proxy=self.handler.proxy) as response:
                    data = await response.json()
                match response.status:
                    case 200:
                        await self.channel.default_exchange.publish(
                            aio_pika.Message(
                                pickle.dumps(
                                    (
                                        data["id"],
                                        data["puuid"],
                                        data["name"],
                                        data["revisionDate"],
                                    )
                                ),
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key="puuid_results_found_%s" % self.platform,
                        )
                        await message.ack()
                        self.logging.debug("200 | %s", url)
                    case 404:
                        self.not_found.append(message.body)
                        await self.channel.default_exchange.publish(
                            aio_pika.Message(
                                message.body,
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key="puuid_results_not_found_%s" % self.platform,
                        )
                        await message.ack()
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
