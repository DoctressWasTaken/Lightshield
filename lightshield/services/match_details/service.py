import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

import aiohttp


class Platform:
    service_running = False
    match_updates = None
    match_updates_faulty = None
    worker_count = 20
    task_queue = None
    proxy_endpoint = None

    def __init__(self, region, platform, config, handler):
        self.region = region
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger(platform)

        self.worker_count = 50 // len(getattr(config.statics.mapping, region))
        self.matches = []

        self.found = []
        self.missing = []
        self.summoner_updates = []

        self.output_folder = config.services.match_details.location or os.getcwd()
        self.logging.info("Output folder: %s", self.output_folder)
        self.retry_after = datetime.now()
        self.proxy = handler.proxy
        self.endpoint_url = (
            f"{config.connections.proxy.protocol}://{self.region.lower()}.api.riotgames.com"
            f"/lol/match/v5/matches/%s_%s"
        )

    async def run(self):
        while not self.handler.is_shutdown:
            async with self.handler.postgres.acquire() as connection:
                async with connection.transaction():
                    await self.pull_tasks(connection)
                    async with aiohttp.ClientSession() as session:
                        await asyncio.gather(
                            *[
                                asyncio.create_task(self.worker(session, _))
                                for _ in range(self.worker_count)
                            ]
                        )
                    if self.handler.is_shutdown:
                        return
                    await self.flush(connection)

    async def pull_tasks(self, connection):
        """Get match_ids from the db."""
        while not self.handler.is_shutdown:
            self.matches = await connection.fetch(
                """
               SELECT  match_id
               FROM "match_{platform:s}"
                   WHERE details IS NULL
                    AND find_fails < 10
                ORDER BY find_fails 
               LIMIT $1
               FOR UPDATE 
               SKIP LOCKED
               """.format(
                    platform=self.platform.lower()
                ),
                self.worker_count * 10,
            )
            if not self.matches:
                self.logging.info("Found no tasks")
                await asyncio.sleep(5)
                continue
            return

    async def flush(self, connection):
        """Insert results from requests into the db."""
        if self.found:
            query = await connection.prepare(
                """UPDATE "match_{platform:s}"
                SET queue = $1,
                    timestamp = $2,
                    version = $3,
                    duration = $4,
                    win = $5,
                    details = TRUE
                    WHERE match_id = $6
                """.format(
                    platform=self.platform.lower()
                ),
            )
            await query.executemany(self.found)
            self.found = []
        if self.missing:
            prep = await connection.prepare(
                """UPDATE "match_{platform:s}"
                                   SET find_fails = find_fails + 1
                                   WHERE match_id = $1
                                """.format(
                    platform=self.platform.lower()
                )
            )
            await prep.executemany([(entry["match_id"]) for entry in self.missing])
            self.missing = []

        if self.summoner_updates:
            query = await connection.prepare(
                """UPDATE summoner
                    SET last_activity = $1,
                        platform = $2,
                        name = $3
                    WHERE puuid = $4 
                        AND last_activity < $1
                """
            )
            await query.executemany(self.summoner_updates)
            self.summoner_updates = []

    async def handle_response(self, response, task):
        if response["info"]["queueId"] == 0:
            self.missing.append(task)
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
        for player in response["info"]["participants"]:
            self.summoner_updates.append(
                (last_activity, self.platform, player["summonerName"], player["puuid"])
            )
        day = creation.strftime("%Y_%m_%d")
        patch_int = int("".join([el.zfill(2) for el in patch.split(".")]))
        # Match Update
        self.found.append(
            [
                queue,
                creation,
                patch_int,
                game_duration,
                win,
                task[0],
            ]
        )
        # Saving
        path = os.path.join(self.output_folder, "details", patch, day, self.platform)
        if not os.path.exists(path):
            os.makedirs(path)
        filename = os.path.join(path, "%s_%s.json" % (self.platform, task[0]))
        if not os.path.isfile(filename):
            with open(
                filename,
                "w+",
            ) as file:
                file.write(json.dumps(response))

    async def worker(self, session, offset):
        """Execute requests."""
        await asyncio.sleep(offset * 0.05)
        task = None
        while not self.handler.is_shutdown:
            if not task:
                if not self.matches:
                    return
                task = self.matches.pop()
            url = self.endpoint_url % (self.platform, task[0])
            async with session.get(url, proxy=self.proxy) as response:
                match response.status:
                    case 200:
                        if self.handler.is_shutdown:
                            return
                        await self.handle_response(await response.json(), task)
                        task = None
                    case 404:
                        self.missing.append(task)
                        task = None
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
