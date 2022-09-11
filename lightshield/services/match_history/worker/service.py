import asyncio
import logging
import pickle
import types
from datetime import datetime, timedelta

import aiohttp

from lightshield.rabbitmq_defaults import QueueHandler
from lightshield.services.match_history import queries


class Platform:
    running = False
    _runner = None
    results_queue = None
    cancel_consume = None

    def __init__(self, region, platform, config, handler, semaphore):
        self.config = config
        self.region = region
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger("%s" % platform)
        self.service = config.services.match_history
        self.retry_after = datetime.now()
        # Internal queue for updates
        self.summoner_updates = asyncio.Queue()

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
            self.session = aiohttp.ClientSession(
                connector=conn, headers={"ratelimit": str(self.service.ratelimit)}
            )
        else:
            self.session = aiohttp.ClientSession(connector=conn)

    async def process_page(self, url, task_instance):
        async with self.semaphore:
            if self.handler.is_shutdown:
                await message.reject(requeue=True)
                return
            sleep = asyncio.create_task(asyncio.sleep(1))
            async with self.session.get(
                url, proxy=self.config.proxy.string
            ) as response:
                data = await response.json()
                await sleep
        match response.status:
            case 200:
                task_instance.calls_to_make.pop(0)
                if not data:
                    return []
                return data
            case 404:
                task_instance.is_404 = True
            case 429:
                await asyncio.sleep(0.5)
            case 430:
                self.retry_after = datetime.fromtimestamp(data["Retry-At"])
            case _:
                await asyncio.sleep(0.01)
        return None

    async def process_tasks(self, message):
        puuid, latest_match, latest_history_update = pickle.loads(message.body)
        now = datetime.now()
        newer_than = now - timedelta(days=self.service.history.days)
        newer_than_tst = int(newer_than.timestamp())
        url = self.endpoint_url % puuid
        url += "&startTime=%s" % newer_than_tst
        task_instance = types.SimpleNamespace()

        task_instance.calls_to_make = [
            url + "&start=%s" % start_index
            for start_index in range(0, self.service.history.matches, 100)
        ]
        task_instance.is_404 = False
        matches = []

        found_latest = False
        newest_match = None
        while (
            task_instance.calls_to_make
            and not task_instance.is_404
            and not self.handler.is_shutdown
            and not found_latest
        ):
            seconds = (self.retry_after - datetime.now()).total_seconds()
            if seconds >= 0.1:
                await asyncio.sleep(seconds)
            try:
                if response_matches := await self.process_page(
                    task_instance.calls_to_make[0], task_instance
                ):
                    if not newest_match:
                        newest_match = int(response_matches[0].split("_")[1])
                    for match in response_matches:
                        platform, id = match.split("_")
                        if id == latest_match:
                            found_latest = True
                            break
                        if self.service.queue:
                            matches.append((platform, int(id), self.service.queue))
                        else:
                            matches.append((platform, int(id)))

            except aiohttp.ClientProxyConnectionError:
                await asyncio.sleep(0.01)
                continue
            except aiohttp.ContentTypeError as err:
                self.logging.error(err)
                await asyncio.sleep(0.01)
                continue

        if not newest_match:
            newest_match = latest_match
        package = {
            "matches": list(set(matches)),
            "summoner": [puuid, newest_match, now],
        }
        await self.results_queue.send_task(pickle.dumps(package), persistent=True)
        await message.ack()

    async def shutdown(self):
        await self.cancel_consume()

    async def run(self, results_queue):
        self.logging.info("Started worker")
        try:
            task_queue = QueueHandler("match_history_tasks_%s" % self.platform)
            await task_queue.init(
                durable=True,
                prefetch_count=100,
                connection=self.handler.pika,
            )
            self.results_queue = results_queue
            self.cancel_consume = await task_queue.consume_tasks(self.process_tasks)
            while not self.handler.is_shutdown:
                await asyncio.sleep(1)

            await asyncio.sleep(10)
        finally:
            self.logging.info("Exited worker")
