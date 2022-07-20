import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

import aiohttp
from lightshield.services.match_details import queries
from lightshield.rabbitmq_defaults import QueueHandler
import pickle


class Platform:
    service_running = False
    match_updates = None
    match_updates_faulty = None
    task_queue = None
    proxy_endpoint = None
    matches_queue_200 = matches_queue_404 = summoner_queue = None

    def __init__(self, region, platform, config, handler, semaphore):
        self.region = region
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger(platform)
        self.semaphore = semaphore

        self.output_folder = config.services.match_details.location or os.getcwd()
        self.retry_after = datetime.now()
        self.proxy = handler.proxy
        self.endpoint_url = (
            f"{config.connections.proxy.protocol}://{self.region.lower()}.api.riotgames.com"
            f"/lol/match/v5/matches/%s_%s"
        )

    async def run(self):
        task_queue = QueueHandler("match_details_tasks_%s" % self.platform)
        await task_queue.init(
            durable=True, prefetch_count=100, connection=self.handler.pika
        )

        self.matches_queue_200 = QueueHandler(
            "match_details_results_matches_200_%s" % self.platform
        )
        await self.matches_queue_200.init(durable=True, connection=self.handler.pika)

        self.matches_queue_404 = QueueHandler(
            "match_details_results_matches_404_%s" % self.platform
        )
        await self.matches_queue_404.init(durable=True, connection=self.handler.pika)

        self.summoner_queue = QueueHandler(
            "match_details_results_summoners_%s" % self.platform
        )
        await self.summoner_queue.init(durable=True, connection=self.handler.pika)

        cancel_consume = await task_queue.consume_tasks(self.process_tasks)
        conn = aiohttp.TCPConnector(limit=0)
        self.session = aiohttp.ClientSession(connector=conn)

        while not self.handler.is_shutdown:
            await asyncio.sleep(1)

        await cancel_consume()
        await asyncio.sleep(10)

    async def process_tasks(self, message):
        async with message.process(ignore_processed=True):
            matchId = int(message.body.decode("utf-8"))
            now = datetime.now()
            now_tst = int(now.timestamp())
            url = self.endpoint_url % (self.platform, matchId)
            seconds = (self.retry_after - datetime.now()).total_seconds()
            if seconds >= 0.1:
                await asyncio.sleep(seconds)
            try:
                async with self.semaphore:
                    if self.handler.is_shutdown:
                        await message.reject(requeue=True)
                        return
                    sleep = asyncio.create_task(asyncio.sleep(1))
                    async with self.session.get(url, proxy=self.proxy) as response:
                        data, _ = await asyncio.gather(response.json(), sleep)
                match response.status:
                    case 200:
                        await self.parse_response(data, matchId)
                        await message.ack()
                        return
                    case 404:
                        await self.matches_queue_404.send_tasks([str(matchId).encode()])
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
            await message.reject(requeue=True)

    async def flush(self, connection):
        """Insert results from requests into the db."""
        if self.found:
            query = await connection.prepare(
                queries.flush_found[self.handler.connection.type].format(
                    platform=self.platform,
                    platform_lower=self.platform.lower(),
                    schema=self.handler.connection.schema,
                ),
            )
            await query.executemany(self.found)
            self.found = []
        if self.missing:
            prep = await connection.prepare(
                queries.flush_missing[self.handler.connection.type].format(
                    platform=self.platform,
                    platform_lower=self.platform.lower(),
                    schema=self.handler.connection.schema,
                )
            )
            await prep.executemany([(entry["match_id"]) for entry in self.missing])
            self.missing = []

        if self.summoner_updates:
            query = await connection.prepare(
                queries.flush_updates[self.handler.connection.type].format(
                    schema=self.handler.connection.schema,
                )
            )
            await query.executemany(self.summoner_updates)
            self.summoner_updates = []

    async def parse_response(self, response, matchId):
        if response["info"]["queueId"] == 0:
            await self.matches_queue_404.send_tasks([str(matchId).encode()])
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
        summoner_updates = []
        for player in response["info"]["participants"]:
            summoner_updates.append(
                pickle.dumps(
                    (
                        last_activity,
                        self.platform,
                        player["summonerName"],
                        player["puuid"],
                    )
                )
            )
        await self.summoner_queue.send_tasks(summoner_updates)
        day = creation.strftime("%Y_%m_%d")
        patch_int = int("".join([el.zfill(2) for el in patch.split(".")]))
        # Match Update
        await self.matches_queue_200.send_tasks(
            [
                pickle.dumps(
                    (
                        queue,
                        creation,
                        patch_int,
                        game_duration,
                        win,
                        matchId,
                    )
                )
            ]
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