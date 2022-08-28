import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

import aiohttp
from lightshield.rabbitmq_defaults import QueueHandler
import pickle

from lightshield.services.match_details import queries


class Platform:
    task_queue = None
    summoner_queue = None

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

    async def run(self):
        task_queue = QueueHandler("match_details_tasks_%s" % self.platform)
        await task_queue.init(
            durable=True, prefetch_count=100, connection=self.handler.pika
        )
        self.summoner_queue = QueueHandler("match_details_results_%s" % self.platform)
        await self.summoner_queue.init(durable=True, connection=self.handler.pika)
        inserter = asyncio.create_task(self.update_matches())

        cancel_consume = await task_queue.consume_tasks(self.process_tasks)
        conn = aiohttp.TCPConnector(limit=0)
        self.session = aiohttp.ClientSession(connector=conn)
        while not self.handler.is_shutdown:
            await asyncio.sleep(1)
        await asyncio.gather(
            inserter,
            asyncio.create_task(cancel_consume()),
            asyncio.create_task(asyncio.sleep(10)),
        )

    async def update_matches(self):
        """Batch insert updates into postgres and marks the rabbitmq messages as completed."""
        self.logging.info("Started matches updater.")
        while True:
            if (
                self.match_200.qsize() + self.match_404.qsize() >= 50
                or self.handler.is_shutdown
            ):
                matches_200 = []
                while True:
                    try:
                        matches_200.append(self.match_200.get_nowait())
                        self.match_200.task_done()
                    except:
                        break
                matches_404 = []
                while True:
                    try:
                        matches_404.append(self.match_404.get_nowait())
                        self.match_404.task_done()
                    except:
                        break
                async with self.handler.db.acquire() as connection:
                    async with connection.transaction():
                        prep = await connection.prepare(
                            queries.flush_found.format(
                                platform_lower=self.platform.lower(),
                                platform=self.platform,
                            )
                        )
                        await prep.executemany([task["data"] for task in matches_200])
                        for task in matches_200:
                            await task["message"].ack()

                        prep = await connection.prepare(
                            queries.flush_missing.format(
                                platform_lower=self.platform.lower(),
                                platform=self.platform,
                            )
                        )
                        await prep.executemany([task["data"] for task in matches_404])
                        for task in matches_404:
                            await task["message"].ack()

            if self.handler.is_shutdown:
                break
            await asyncio.sleep(2)

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
                sleep = asyncio.create_task(asyncio.sleep(1))
                async with self.session.get(url, proxy=self.proxy) as response:
                    data, _ = await asyncio.gather(response.json(), sleep)
            match response.status:
                case 200:
                    await self.parse_response(data, matchId, message)
                    return
                case 404:
                    await self.match_404.put(
                        {"data": [str(matchId)], "message": message}
                    )
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

    async def parse_response(self, response, matchId, message):
        if response["info"]["queueId"] == 0:
            await self.match_404.put({"data": [str(matchId)], "message": message})
            return

        queue = response["info"]["queueId"]
        creation = datetime.fromtimestamp(response["info"]["gameCreation"] // 1000)
        patch = ".".join(response["info"]["gameVersion"].split(".")[:2])
        if (
            "gameStartTimestamp" in response["info"]
            and "gameEndTimestamp" in response["info"]
        ):
            game_duration = (
                response["info"]["gameEndTimestamp"]
                - response["info"]["gameStartTimestamp"]
            )
        else:
            game_duration = response["info"]["gameDuration"]
        if game_duration >= 30000:
            game_duration //= 1000
        win = (response["info"]["teams"][0]["teamId"] == 100) == (
            not response["info"]["teams"][0]["win"]
        )

        # Summoner updates
        last_activity = creation + timedelta(seconds=game_duration)
        await self.summoner_queue.send_task(
            pickle.dumps(
                [
                    (
                        last_activity,
                        self.platform,
                        player["summonerName"],
                        player["puuid"],
                    )
                    for player in response["info"]["participants"]
                ]
            )
        )

        day = creation.strftime("%Y_%m_%d")
        patch_int = int("".join([el.zfill(2) for el in patch.split(".")]))
        # Match Update
        await self.match_200.put(
            {
                "data": (
                    queue,
                    creation,
                    patch_int,
                    game_duration,
                    win,
                    matchId,
                ),
                "message": message,
            }
        )
        # Saving
        path = os.path.join(self.output_folder, "details", patch, day, self.platform)
        if not os.path.exists(path):
            os.makedirs(path)
        filename = os.path.join(path, "%s_%s.json" % (self.platform, matchId))
        if not os.path.isfile(filename):
            with open(
                filename,
                "w+",
            ) as file:
                file.write(json.dumps(response))
