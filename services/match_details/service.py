import asyncio
import json
import logging
import os
from asyncio import Queue
from datetime import datetime, timedelta

import aiohttp

from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
)


class Platform:
    service_running = False
    match_updates = None
    match_updates_faulty = None
    worker_count = 20
    task_queue = None
    proxy_endpoint = None

    def __init__(self, region, platforms, handler):
        self.name = region
        self.platforms = platforms
        self.handler = handler
        self.logging = logging.getLogger("%s" % region)
        self._worker = []  # Worker promises
        self.updater = None  # Updater promise
        self.retry_after = datetime.now()
        self.proxy = handler.proxy
        self.endpoint_url = (
            f"https://{self.name}.api.riotgames.com/lol/match/v5/matches/%s_%s"
        )

    async def init(self):
        """Init background runner."""
        self.task_queue = Queue()
        self.match_updates = Queue()
        self.match_updates_faulty = Queue()
        self.logging.info("Ready.")

    async def shutdown(self):
        self.logging.info("Shutdown")
        self.service_running = False
        await asyncio.gather(*self._worker, self.updater)
        await self.flush_tasks()

    async def start(self):
        """Start the service calls."""
        if not self.service_running:
            self.service_running = True
            self.logging.info("Started service calls.")
            self.proxy_endpoint = await self.handler.proxy.get_endpoint(
                server=self.name, zone="match-details-v5"
            )
            self.updater = asyncio.create_task(self.task_updater())
            self._worker = [
                asyncio.create_task(self.worker()) for _ in range(self.worker_count)
            ]

    async def stop(self):
        """Halt the service calls."""
        if self.service_running:
            self.service_running = False
            self.logging.info("Stopped service calls.")
            await self.updater
            for worker in self._worker:
                worker.cancel()
            try:
                await asyncio.gather(*self._worker)
            except asyncio.CancelledError:
                pass
            await self.flush_tasks()

    async def task_updater(self):
        """Pull new tasks when the list is empty."""
        self.logging.debug("Task Updater initiated.")
        while self.service_running:
            if self.match_updates.qsize() >= 200:
                await self.flush_tasks()
                continue
            if self.task_queue.qsize() > 200:
                await asyncio.sleep(5)
                continue
            async with self.handler.postgres.acquire() as connection:
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
                    self.task_queue.qsize(),
                    self.task_queue.qsize() + len(entries),
                )
                if len(entries) == 0:
                    await asyncio.sleep(30)
                    await self.flush_tasks()
                    pass

                for entry in entries:
                    await self.task_queue.put([entry["platform"], entry["match_id"]])

    async def worker(self):
        """Execute requests."""
        async with aiohttp.ClientSession(
            headers={"X-Riot-Token": self.handler.api_key}
        ) as session:
            while self.service_running:
                task = await self.task_queue.get()
                try:
                    #   Success
                    url = self.endpoint_url % (task[0], task[1])
                    response = await self.proxy_endpoint.request(url, session)
                    if response["info"]["queueId"] == 0:
                        raise NotFoundException  # TODO: queue 0 means its a custom, so it should be set to max retries immediatly

                    # Extract relevant data
                    queue = response["info"]["queueId"]
                    creation = datetime.fromtimestamp(
                        response["info"]["gameCreation"] // 1000
                    )
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
                    win = [
                        team["win"]
                        for team in response["info"]["teams"]
                        if team["teamId"] == 100
                    ][0]
                    players = []
                    for player in response["info"]["participants"]:
                        players.append(
                            [
                                task[1],
                                player["puuid"],
                                player["championId"]
                                if player["championId"] < 30000
                                else -1,
                                player["teamId"] != 100,
                                # TODO: Add lane: 'lane': player['teamPosition'], (add as a enum in postgres first)
                            ]
                        )
                    day = creation.strftime("%Y_%m_%d")
                    patch_int = int("".join([el.zfill(2) for el in patch.split(".")]))
                    path = os.path.join(os.sep, "data", "details", patch, day, task[0])
                    if not os.path.exists(path):
                        os.makedirs(path)
                    filename = os.path.join(path, "%s_%s.json" % (task[0], task[1]))
                    if not os.path.isfile(filename):
                        with open(
                            filename,
                            "w+",
                        ) as file:
                            file.write(json.dumps(response))
                    # del response
                    package = {
                        "match": [
                            queue,
                            creation,
                            patch_int,
                            game_duration,
                            win,
                            task[0],
                            task[1],
                        ],
                        "participant": players,
                    }
                    await self.match_updates.put(package)
                    self.task_queue.task_done()
                except LimitBlocked as err:
                    self.retry_after = datetime.now() + timedelta(
                        seconds=err.retry_after
                    )
                    await self.task_queue.put(task)
                    self.task_queue.task_done()
                except RatelimitException as err:
                    self.logging.error("Ratelimit")
                    await self.task_queue.put(task)
                    self.task_queue.task_done()
                except Non200Exception as err:
                    self.logging.error("Others")
                    await self.task_queue.put(task)
                    self.task_queue.task_done()
                except NotFoundException:
                    await self.match_updates_faulty.put([task[0], task[1]])
                    self.task_queue.task_done()
                except Exception as err:
                    self.logging.exception("General Exception")
                    await self.task_queue.put(task)
                    self.task_queue.task_done()

    async def flush_tasks(self):
        """Insert results from requests into the db."""
        match_updates = []
        while True:
            try:
                match_updates.append(self.match_updates.get_nowait())
                self.match_updates.task_done()
            except asyncio.QueueEmpty:
                break
        match_not_found = []
        while True:
            try:
                match_not_found.append(self.match_updates_faulty.get_nowait())
                self.match_updates_faulty.task_done()
            except asyncio.QueueEmpty:
                break

        async with self.handler.postgres.acquire() as connection:
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
                    await connection.execute(
                        """UPDATE %s.match
                            SET find_fails = find_fails + 1,
                                reserved_details = current_date + INTERVAL '10 minute'
                            WHERE platform::varchar || '_' || match_id::varchar = any($1::varchar[])
                        """
                        % self.name,
                        ["%s_%s" % match for match in match_not_found],
                    )

        if match_updates or match_not_found:
            self.logging.info(
                "Flushing %s match_updates (%s not found).",
                len(match_updates) + len(match_not_found),
                len(match_not_found),
            )
