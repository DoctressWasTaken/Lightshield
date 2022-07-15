import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp

from lightshield.services.match_history import queries


class Platform:
    running = False
    _runner = None

    def __init__(self, region, platform, config, handler):
        self.region = region
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger("%s" % platform)
        self.service = config.services.match_history

        self.proxy = handler.proxy
        self.endpoint_url = (
            f"{config.connections.proxy.protocol}://{self.region.lower()}.api.riotgames.com"
            f"/lol/match/v5/matches/by-puuid/%s/ids"
            f"?count=100"
        )
        if self.service.type:
            self.endpoint_url += "&type=%s" % self.service.type
        if self.service.queue:
            self.endpoint_url += "&queue=%s" % self.service.queue

        self.matches = []
        self.updated_players = []

    async def insert_matches(self, connection):
        by_platform = {}
        for match in list(set(self.matches)):
            platform, id = match.split("_")
            if platform not in by_platform:
                by_platform[platform] = []
            by_platform[platform].append(int(id))

        for platform in by_platform.keys():
            if self.service.queue:
                data = [
                    [platform, id, self.service.queue] for id in by_platform[platform]
                ]
                await connection.executemany(
                    queries.insert_queue_known[self.handler.connection.type].format(
                        platform=self.platform,
                        platform_lower=self.platform.lower(),
                        schema=self.handler.connection.schema,
                    ),
                    data,
                )
            else:
                data = [[platform, id] for id in by_platform[platform]]
                await connection.executemany(
                    queries.insert_queue_unknown[self.handler.connection.type].format(
                        platform=self.platform,
                        platform_lower=self.platform.lower(),
                        schema=self.handler.connection.schema,
                    ),
                    data,
                )
        self.logging.info("Inserted %s matches.", len(self.matches))

    async def insert_players(self, connection):
        prep = await connection.prepare(
            queries.update_players[self.handler.connection.type].format(
                platform=self.platform,
                platform_lower=self.platform.lower(),
                schema=self.handler.connection.schema,
            )
        )
        await prep.executemany(self.updated_players)
        self.logging.info("Updated %s players.", len(self.updated_players))

    async def run(self):
        semaphore = asyncio.Semaphore(20)
        while not self.handler.is_shutdown:
            async with self.handler.db.acquire() as connection:
                self.matches = []
                players = await connection.fetch(
                    queries.reserve[self.handler.connection.type].format(
                        platform=self.platform,
                        platform_lower=self.platform.lower(),
                        schema=self.handler.connection.schema,
                        min_wait=self.service.min_wait,
                    ),
                    self.service.min_wait,
                )
                if not players:
                    await asyncio.sleep(5)
                    continue
                async with aiohttp.ClientSession() as session:
                    await asyncio.gather(
                        *[
                            asyncio.create_task(self.worker(player, semaphore, session))
                            for player in players
                        ]
                    )
                if self.handler.is_shutdown:
                    return
                if self.matches:
                    await self.insert_matches(connection=connection)
                if self.updated_players:
                    await self.insert_players(connection=connection)

    async def worker(self, player, semaphore, session):
        now = datetime.now() - timedelta(days=self.service.history.days)
        now_tst = int(now.timestamp())
        url = self.endpoint_url % player["puuid"]
        url += "&startTime=%s" % now_tst
        start_index = 0
        is_404 = False
        latest_match = None
        while (
            start_index < self.service.history.matches
            and not is_404
            and not self.handler.is_shutdown
        ):
            task_url = url + "&start=%s" % start_index
            async with semaphore:
                try:
                    async with session.get(task_url, proxy=self.proxy) as response:
                        match response.status:
                            case 200:
                                matches = await response.json()
                                if not matches:
                                    break
                                if start_index == 0:
                                    latest_match = int(matches[0].split("_")[1])
                                start_index += 100
                                self.matches += matches
                            case 404:
                                is_404 = True
                            case 429:
                                await asyncio.sleep(0.5)
                            case 430:
                                data = await response.json()
                                wait_until = datetime.fromtimestamp(data["Retry-At"])
                                seconds = (wait_until - datetime.now()).total_seconds()
                                seconds = max(0.1, seconds)
                                await asyncio.sleep(seconds)
                            case _:
                                await asyncio.sleep(0.1)
                except aiohttp.ClientProxyConnectionError:
                    await asyncio.sleep(0.1)
                    continue
        self.updated_players.append([player["puuid"], latest_match, now])
