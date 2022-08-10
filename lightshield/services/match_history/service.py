import asyncio
import logging
import pickle
from datetime import datetime, timedelta

import aiohttp

from lightshield.rabbitmq_defaults import QueueHandler


class Platform:
    running = False
    _runner = None
    matches_queue = summoner_queue = None

    def __init__(self, region, platform, config, handler, semaphore):
        self.config = config
        self.region = region
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger("%s" % platform)
        self.service = config.services.match_history
        self.retry_after = datetime.now()

        # Region crossing semaphore to limit single service output
        self.semaphore = semaphore

        self.endpoint_url = (
            f"{config.proxy.protocol}://{self.region.lower()}.api.riotgames.com"
            f"/lol/match/v5/matches/by-puuid/%s/ids"
            f"?count=100"
        )
        if self.service.type:
            self.endpoint_url += "&type=%s" % self.service.type
        if self.service.queue:
            self.endpoint_url += "&queue=%s" % self.service.queue

        conn = aiohttp.TCPConnector(limit=0)
        if self.service.ratelimit:
            self.session = aiohttp.ClientSession(connector=conn, headers={'ratelimit': str(self.service.ratelimit)})
        else:
            self.session = aiohttp.ClientSession(connector=conn)

    async def process_tasks(self, message):
        async with message.process(ignore_processed=True):
            puuid, latest_match, latest_history_update = pickle.loads(message.body)
            self.logging.info(pickle.loads(message.body))
            now = datetime.now()
            newer_than = now - timedelta(days=self.service.history.days)
            newer_than_tst = int(newer_than.timestamp())
            url = self.endpoint_url % puuid
            url += "&startTime=%s" % newer_than_tst

            calls_to_make = [url + "&start=%s" % start_index
                             for start_index in range(0, self.service.history.matches, 100)
                             ]
            is_404 = False
            newest_match = None
            matches = []
            found_latest = False
            while (
                    calls_to_make
                    and not is_404
                    and not self.handler.is_shutdown
                    and not found_latest
            ):
                seconds = (self.retry_after - datetime.now()).total_seconds()
                if seconds >= 0.1:
                    await asyncio.sleep(seconds)
                try:
                    async with self.semaphore:
                        if self.handler.is_shutdown:
                            await message.reject(requeue=True)
                            return
                        sleep = asyncio.create_task(asyncio.sleep(1))
                        async with self.session.get(
                                calls_to_make[0], proxy=self.config.proxy.string
                        ) as response:
                            data = await response.json()
                            await sleep
                    match response.status:
                        case 200:
                            if not data:
                                break
                            if not newest_match:
                                newest_match = int(data[0].split("_")[1])
                            calls_to_make.pop(0)
                            for match in data:
                                platform, id = match.split("_")
                                if id == latest_match:
                                    found_latest = True
                                    break
                                if self.service.queue:
                                    matches.append(
                                        (platform, int(id), self.service.queue)
                                    )
                                else:
                                    matches.append((platform, int(id)))
                        case 404:
                            is_404 = True
                        case 429:
                            await asyncio.sleep(0.5)
                        case 430:
                            self.retry_after = datetime.fromtimestamp(data["Retry-At"])
                        case _:
                            await asyncio.sleep(0.01)
                except aiohttp.ClientProxyConnectionError:
                    await asyncio.sleep(0.01)
                    continue
            if not newest_match:
                newest_match = latest_match
            matches = list(set(matches))
            if matches:
                await self.matches_queue.send_task(pickle.dumps(matches), persistent=True)
                self.logging.info(
                    "Updated user %s, found %s matches", puuid, len(matches)
                )
            else:
                self.logging.info("Updated user %s, found no matches.", puuid)
            await self.summoner_queue.send_task(pickle.dumps((puuid, newest_match, now)))
            await message.ack()


    async def run(self):
        task_queue = QueueHandler("match_history_tasks_%s" % self.platform)
        await task_queue.init(
            durable=True, prefetch_count=100, connection=self.handler.pika
        )

        self.matches_queue = QueueHandler(
            "match_history_results_matches_%s" % self.platform
        )
        await self.matches_queue.init(durable=True, connection=self.handler.pika)

        self.summoner_queue = QueueHandler(
            "match_history_results_summoners_%s" % self.platform
        )
        await self.summoner_queue.init(durable=True, connection=self.handler.pika)

        cancel_consume = await task_queue.consume_tasks(self.process_tasks)

        while not self.handler.is_shutdown:
            await asyncio.sleep(1)

        await cancel_consume()
        await asyncio.sleep(10)
