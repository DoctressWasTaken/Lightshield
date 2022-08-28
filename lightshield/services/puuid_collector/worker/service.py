import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp
import aio_pika
import pickle
from lightshield.services.puuid_collector import queries
from lightshield.rabbitmq_defaults import QueueHandler


class Platform:
    _runner = None
    channel = None
    results_queue = None

    def __init__(self, platform, handler):
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger("%s" % platform)
        # Output queues
        self.results = asyncio.Queue()
        self.not_found = asyncio.Queue()

        self.retry_after = datetime.now()
        self.endpoint_url = f"{handler.protocol}://{platform.lower()}.api.riotgames.com/lol/summoner/v4/summoners/%s"
        self.semaphore = asyncio.Semaphore(10)
        conn = aiohttp.TCPConnector(limit=0)
        self.session = aiohttp.ClientSession(connector=conn)
        self.active = 0

    async def run(self):
        """Main object loop."""
        task_queue = QueueHandler("puuid_tasks_%s" % self.platform)
        await task_queue.init(
            durable=True, prefetch_count=100, connection=self.handler.pika
        )
        self.results_queue = QueueHandler("puuid_results_%s" % self.platform)
        await self.results_queue.init(durable=True, connection=self.handler.pika)

        inserter = asyncio.create_task(self.insert_updates())
        cancel_consume = await task_queue.consume_tasks(self.task_handler)

        while not self.handler.is_shutdown:
            await asyncio.sleep(1)
        await task_queue.cancel(consumer)
        while self.active > 0:
            await asyncio.sleep(0.1)
        await asyncio.gather(
            inserter,
            asyncio.create_task(cancel_consume()),
            asyncio.create_task(asyncio.sleep(10)),
        )
    async def insert_updates(self):
        """Batch insert updates into postgres and marks the rabbitmq messages as completed."""
        self.logging.info("Started ranking updater.")
        while True:
            if (
                self.results.qsize() + self.not_found.qsize() >= 50
                or self.handler.is_shutdown
            ):
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
                        if found:
                            prep = await connection.prepare(
                                queries.update_ranking.format(
                                    platform=self.platform,
                                    platform_lower=self.platform.lower(),
                                )
                            )
                            await prep.executemany([task["data"] for task in found])
                            for task in found:
                                await task["message"].ack()
                        if not_found:
                            await connection.execute(
                                queries.missing_summoner.format(
                                    platform=self.platform,
                                    platform_lower=self.platform.lower(),
                                ),
                                [task["data"] for task in not_found],
                            )
                            for task in not_found:
                                await task["message"].ack()
            if self.handler.is_shutdown:
                break
            await asyncio.sleep(2)

    async def task_handler(self, message):
        """Execute requests."""
        seconds = (self.retry_after - datetime.now()).total_seconds()
        if seconds >= 0.1:
            await asyncio.sleep(seconds)
        account_id = message.body.decode("utf-8")
        url = self.endpoint_url % account_id
        try:
            async with self.semaphore:
                if self.handler.is_shutdown:
                    await message.reject(requeue=True)
                    return
                sleep = asyncio.create_task(asyncio.sleep(1))
                async with self.session.get(url, proxy=self.handler.proxy) as response:
                    data, _ = await asyncio.gather(response.json(), sleep)
            match response.status:
                case 200:
                    # Summoner insert goes into the next rabbitmq queue
                    await self.results_queue.send_task(
                        pickle.dumps(
                                (
                                    data["puuid"],
                                    data["name"],
                                    data["revisionDate"],
                                )
                            ),
                        persistent=True
                    )
                    # Ranking update goes into the internal queue
                    await self.results.put(
                        {"data": [data["id"], data["puuid"]], "message": message}
                    )
                    self.logging.debug("200 | %s", url)
                    return
                case 404:
                    await self.not_found.put(
                        {"data": account_id, "message": message}
                    )
                    self.logging.debug("404 | %s", url)
                    return
                case 429:
                    self.retry_after = datetime.now() + timedelta(seconds=0.5)
                case 430:
                    self.retry_after = datetime.fromtimestamp(data["Retry-At"])
        except aiohttp.ClientProxyConnectionError:
            pass
        except Exception as err:
            self.logging.error(err)
        # If didnt return reject (200 + 404 return before)
        await message.reject(requeue=True)
