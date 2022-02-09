import asyncio
import json
import logging
import os
from asyncio import Queue
from datetime import datetime, timedelta

import aiohttp
import asyncpg

from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
)


class Platform:
    running = False
    _runner = None
    results = None
    result_not_found = None
    worker_count = 5

    def __init__(self, region, platforms, handler):
        self.name = region
        self.platforms = platforms
        self.handler = handler
        self.logging = logging.getLogger("%s" % region)
        self.tasks = []  # List of summonerIds
        self._worker = []  # Worker promises
        self.updater = None  # Updater promise
        self.retry_after = datetime.now()
        self.proxy = handler.proxy
        self.endpoint_url = (
            f"https://{self.name}.api.riotgames.com/lol/match/v5/matches/%s_%s"
        )
        self.postgres_params = {
            'host': "postgres",
            'port': 5432,
            'user': "postgres",
            'database': "lightshield"
        }

    async def init(self):
        """Init background runner."""
        self.results = Queue()
        self.result_not_found = Queue()
        self.endpoint = await self.handler.proxy.get_endpoint(
            server=self.name, zone="match-details-v5"
        )
        self.logging.info("Ready.")

    async def shutdown(self):
        self.logging.info("Shutdown")
        self.running = False
        await asyncio.gather(*self._worker, self.updater)
        await self.flush_tasks()

    async def start(self):
        """Start the service calls."""
        if not self.running:
            self.running = True
            self.logging.info("Started service calls.")
            self.updater = asyncio.create_task(self.task_updater())
            self._worker = [asyncio.create_task(self.worker()) for _ in range(10)]

    async def stop(self):
        """Halt the service calls."""
        if self.running:
            self.running = False
            self.logging.info("Stopped service calls.")
            await asyncio.gather(*self._worker, self.updater)
            await self.flush_tasks()

    async def task_updater(self):
        """Pull new tasks when the list is empty."""
        self.logging.debug("Task Updater initiated.")
        while self.running:
            if self.results.qsize() >= 100:
                await self.flush_tasks()
            if len(self.tasks) > 200:
                await asyncio.sleep(5)
                continue
            connection = await asyncpg.connect(
                    **self.postgres_params
                )
            try:
                entries = await connection.fetch(
                    """UPDATE %s.match
                            SET reserved_details = current_date + INTERVAL '10 minute'
                            FROM (
                                SELECT  match_id,
                                        platform
                                    FROM %s.match
                                    WHERE details IS NULL
                                    AND find_fails <= 10
                                    AND (reserved_details IS NULL OR reserved_details < current_timestamp)
                                    ORDER BY find_fails, match_id DESC
                                    LIMIT $1
                                    ) selection
                        WHERE match.match_id = selection.match_id
                           AND match.platform = selection.platform
                            RETURNING match.platform, match.match_id
                    """
                    % tuple([self.name for _ in range(2)]),
                    1000,
                )
                self.logging.debug(
                    "Refilling tasks [%s -> %s].",
                    len(self.tasks),
                    len(self.tasks) + len(entries),
                )
                if len(entries) == 0:
                    await asyncio.sleep(30)
                    await self.flush_tasks()
                    pass

                self.tasks += [
                    [entry["platform"], entry["match_id"]] for entry in entries
                ]
            except Exception as err:
                self.logging.error("Here: %s", err)
            finally:
                await connection.close()

    async def fetch(self, params, session):
        """Call and handle response."""
        try:
            #   Success
            url = self.endpoint_url % (params[0], params[1])
            response = await self.endpoint.request(url, session)
            if response["info"]["queueId"] == 0:
                raise NotFoundException  # TODO: queue 0 means its a custom, so it should be set to max retries immediatly

            # Extract relevant data
            queue = response["info"]["queueId"]
            creation = datetime.fromtimestamp(response["info"]["gameCreation"] // 1000)
            patch = ".".join(response["info"]["gameVersion"].split(".")[:2])
            if "gameStartTimestamp" in response["info"]:
                game_duration = (
                        response["info"]["gameEndTimestamp"]
                        - response["info"]["gameStartTimestamp"]
                )
            else:
                game_duration = response["info"]["gameDuration"]
            if game_duration >= 30000:
                game_duration //= 1000
            win = [
                team["win"]
                for team in response["info"]["teams"]
                if team["teamId"] == 100
            ][0]
            players = []
            for player in response["info"]["participants"]:
                players.append(
                    [
                        params[1],
                        player["puuid"],
                        player["championId"] if player["championId"] < 30000 else -1,
                        player["teamId"] != 100,
                        # TODO: Add lane: 'lane': player['teamPosition'], (add as a enum in postgres first)
                    ]
                )
            day = creation.strftime("%Y_%m_%d")
            patch_int = int("".join([el.zfill(2) for el in patch.split(".")]))
            path = os.path.join(os.sep, "data", "details", patch, day, params[0])
            if not os.path.exists(path):
                os.makedirs(path)

            with open(
                    os.path.join(path, "%s_%s.json" % (params[0], params[1])),
                    "w+",
            ) as file:
                json.dump(response, file)
            del response
            self.logging.debug(url)
            package = {
                "match": [
                    queue,
                    creation,
                    patch_int,
                    game_duration,
                    win,
                    params[0],
                    params[1],
                ],
                "participant": players,
            }
            await self.results.put(package)
        except LimitBlocked as err:
            self.retry_after = datetime.now() + timedelta(seconds=err.retry_after)
            return params
        except RatelimitException as err:
            self.logging.error("Ratelimit")
            return params
        except Non200Exception as err:
            self.logging.error("Others")
            return params
        except NotFoundException:
            await self.result_not_found.put([params[0], params[1]])
        except Exception as err:
            self.logging.error("General: %s", err)
            return params

    async def worker(self):
        """Execute requests."""
        targets = []
        while self.running:
            if not self.tasks:
                await asyncio.sleep(5)
                continue
            while len(targets) < 5:
                if not self.tasks:
                    break
                targets.append(self.tasks.pop())
            async with aiohttp.ClientSession(
                    headers={"X-Riot-Token": self.handler.api_key}
            ) as session:
                targets = [
                    target
                    for target in (
                        await asyncio.gather(
                            *[
                                asyncio.create_task(self.fetch(target, session))
                                for target in targets
                            ]
                        )
                    )
                    if target
                ]

    async def flush_tasks(self):
        """Insert results from requests into the db."""
        connection = await asyncpg.connect(
            **self.postgres_params
        )
        try:
            match_updates = []
            while True:
                try:
                    match_updates.append(self.results.get_nowait())
                    self.results.task_done()
                except asyncio.QueueEmpty:
                    break
            match_not_found = []
            while True:
                try:
                    match_not_found.append(self.result_not_found.get_nowait())
                    self.results.task_done()
                except asyncio.QueueEmpty:
                    break

            if match_updates or match_not_found:
                self.logging.info(
                    "Flushing %s match_updates (%s not found).",
                    len(match_updates) + len(match_not_found),
                    len(match_not_found),
                )
            async with connection.transaction():
                if match_updates:
                    matches = [package["match"] for package in match_updates]
                    # Insert match updates
                    query = await connection.prepare(
                        """UPDATE %s.match
                        SET queue = $1,
                            timestamp = $2,
                            version = $3,
                            duration = $4,
                            win = $5,
                            details = TRUE,
                            reserved_details = NULL
                            WHERE platform = $6
                            AND match_id = $7
                        """
                        % self.name,
                    )
                    await query.executemany(matches)

            platforms = {}

            for package in match_updates:
                if package["match"][-2] not in platforms:
                    platforms[package["match"][-2]] = []
                platforms[package["match"][-2]] += package["participant"]

            for platform in platforms:
                await connection.executemany(
                    """INSERT INTO %s.participant
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT DO NOTHING
                    """
                    % platform,
                    platforms[platform],
                )

            if match_not_found:
                query = await connection.prepare(
                    """UPDATE %s.match
                        SET find_fails = find_fails + 1,
                            reserved_details = current_date + INTERVAL '10 minute'
                        WHERE platform = $1
                        AND match_id = $2
                    """
                    % self.name
                )
                await query.executemany(match_not_found)
        finally:
            await connection.close()
