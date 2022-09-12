import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
import time

import aiohttp
from lightshield.rabbitmq_defaults import QueueHandler
import pickle

from lightshield.services.match_details import queries
from lightshield.services.match_details.worker.parse_data import (
    parse_details,
    parse_summoners,
)


class Platform:
    task_queue = None
    summoner_queue = None
    output_queues = None
    cancel_consume = None

    def __init__(self, region, platform, config, handler, semaphore):
        self.region = region
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger(platform)
        self.semaphore = semaphore
        self.service = config.services.match_details
        self.output_folder = self.service.output
        self.retry_after = datetime.now()

        # Internal queue for updates
        self.match_200 = asyncio.Queue()
        self.match_404 = asyncio.Queue()

        self.proxy = handler.proxy
        self.endpoint_url = (
            f"{config.proxy.protocol}://{self.region.lower()}.api.riotgames.com"
            f"/lol/match/v5/matches/%s_%s"
        )
        self.request_counter = {}

    async def run(self, output):
        self.output_queues = output
        task_queue = QueueHandler("match_details_tasks_%s" % self.platform)
        await task_queue.init(
            durable=True, prefetch_count=100, connection=self.handler.pika
        )
        self.cancel_consume = await task_queue.consume_tasks(self.process_tasks)

        conn = aiohttp.TCPConnector(limit=0)
        self.session = aiohttp.ClientSession(connector=conn)

        while not self.handler.is_shutdown:
            await asyncio.sleep(1)

        asyncio.sleep(8)

    async def shutdown(self):
        await self.cancel_consume()

    async def process_tasks(self, message):
        matchId = int(message.body.decode("utf-8"))
        url = self.endpoint_url % (self.platform, matchId)
        seconds = (self.retry_after - datetime.now()).total_seconds()
        if seconds >= 0.1:
            await asyncio.sleep(seconds)
        try:
            if self.handler.is_shutdown:
                await message.reject(requeue=True)
                return
            async with self.semaphore:
                if self.handler.is_shutdown:
                    await message.reject(requeue=True)
                    return
                sleep = asyncio.create_task(asyncio.sleep(1))
                async with self.session.get(url, proxy=self.proxy) as response:
                    data, _ = await asyncio.gather(response.json(), sleep)
            match response.status:
                case 200:
                    match_package = await parse_details(data, matchId, self.platform)
                    summoner_package = await parse_summoners(data, self.platform)
                    await asyncio.gather(
                        *list(
                            filter(
                                None,
                                [
                                    asyncio.create_task(
                                        self.output_queues.data.send_task(
                                            pickle.dumps(data)
                                        )
                                    )
                                    if match_package["found"]
                                    else None,
                                    asyncio.create_task(
                                        self.output_queues.match.send_task(
                                            pickle.dumps(match_package)
                                        )
                                    ),
                                    asyncio.create_task(
                                        self.output_queues.summoners.send_task(
                                            pickle.dumps(summoner_package)
                                        )
                                    ),
                                ],
                            )
                        )
                    )
                    await message.ack()
                    return
                case 404:
                    await self.output_queues.match.send_task(
                        {
                            "found": False,
                            "platform": self.platform,
                            "data": {"matchId": matchId},
                        }
                    )
                    await message.ack()
                    return
                case 429:
                    await asyncio.sleep(0.5)
                case 430:
                    self.retry_after = datetime.fromtimestamp(data["Retry-At"])
                case _:
                    await asyncio.sleep(0.01)
        except aiohttp.ClientProxyConnectionError:
            await asyncio.sleep(0.01)
        # If didnt return reject (200 + 404 return before)
        await message.reject(requeue=True)
