import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp

from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
)


class Platform:
    running = False
    _runner = None

    def __init__(self, region, platforms, handler):
        self.name = region
        self.platforms = platforms
        self.handler = handler
        self.logging = logging.getLogger("%s" % region)
        self.tasks = []  # List of summonerIds
        self.result_matchids = []  # Properly returning entries.
        self.result_summoners = []  # Properly returning entries.
        self._worker = []  # Worker promises
        self.updater = None  # Updater promise
        self.retry_after = datetime.now()
        self.proxy = handler.proxy
        self.endpoint_url = (
            f"https://{self.name}.api.riotgames.com"
            f"/lol/match/v5/matches/by-puuid/%s/ids?count=100&start=%s"
        )

    async def shutdown(self):
        self.logging.info("Shutdown")
        for worker in self._worker:
            worker.cancel()
        self.updater.cancel()
        await self._runner

    async def start(self):
        """Start the service calls."""
        if not self.running:
            self.logging.info("Started service calls.")
        self.running = True

    async def stop(self):
        """Halt the service calls."""
        if self.running:
            self.logging.info("Stopped service calls.")
        self.running = False

    async def init(self):
        """Init background runner."""
        self.endpoint = await self.handler.proxy.get_endpoint(
            server=self.name, zone="match-history-v5"
        )
        self._runner = asyncio.create_task(self.runner())

    async def runner(self):
        """Main object loop."""
        self.updater = asyncio.create_task(self.task_updater())
        self._worker = [asyncio.create_task(self.worker()) for _ in range(10)]
        try:
            await asyncio.gather(*self._worker, self.updater)
        except asyncio.CancelledError:
            await self.flush_tasks(
                list(set(self.result_matchids)), self.result_summoners
            )
            return

    async def task_updater(self):
        """Pull new tasks when the list is empty."""
        while True:
            if len(self.result_matchids) >= 800:
                matches = list(set(self.result_matchids))
                self.result_matchids = []
                summoners = self.result_summoners.copy()
                self.result_summoners = []
                await self.flush_tasks(matches, summoners)
            if not self.running:
                await asyncio.sleep(5)
                continue
            if len(self.tasks) > 50:
                await asyncio.sleep(5)
                continue
            try:
                async with self.handler.postgres.acquire() as connection:
                    entries = await connection.fetch(
                        """UPDATE summoner
                                SET reserved_match_history = current_date + INTERVAL '10 minute'
                                WHERE puuid IN (
                                    SELECT puuid
                                    FROM summoner
                                    WHERE last_platform = any($1::platform[])
                                    ORDER BY CASE WHEN last_updated IS NULL THEN 0 ELSE 1 END, last_updated
                                    LIMIT $2
                                    FOR UPDATE
                                )
                                RETURNING puuid, last_updated, last_match
                        """,
                        self.platforms,
                        200,
                    )
                    self.logging.debug(
                        "Refilling tasks [%s -> %s].",
                        len(self.tasks),
                        len(self.tasks) + len(entries),
                    )
                    if len(entries) == 0:
                        await asyncio.sleep(30)
                        matches = list(set(self.result_matchids))
                        self.result_matchids = []
                        summoners = self.result_summoners.copy()
                        self.result_summoners = []
                        await self.flush_tasks(matches, summoners)

                    self.tasks += entries
            except Exception as err:
                self.logging.error(err)

    async def fetch(self, target, start, session):
        """Call and handle response."""
        url = self.endpoint_url % (target, start)
        try:
            data = await self.endpoint.request(url, session)
            self.result_matchids += data
            if start == 0:
                platform, id = data[0].split("_")
                id = int(id)
                self.result_summoners.append([platform, id, target])
            self.logging.debug(url)
        except LimitBlocked as err:
            self.retry_after = datetime.now() + timedelta(seconds=err.retry_after)
            return start
        except RatelimitException as err:
            self.logging.error("Ratelimit")
            return start
        except Non200Exception as err:
            self.logging.error("Others")
            return start
        except NotFoundException:
            self.logging.error("Not found error.")
        except Exception as err:
            self.logging.error(err)
            return start

    async def update_full(self, target):
        """Update 10 pages for a user."""
        try:
            starts = [100 * i for i in range(10)]
            async with aiohttp.ClientSession(
                headers={"X-Riot-Token": self.handler.api_key}
            ) as session:
                while starts:
                    while (
                        delay := (self.retry_after - datetime.now()).total_seconds()
                    ) > 0:
                        await asyncio.sleep(min(0.1, delay))
                    starts = [
                        start
                        for start in (
                            await asyncio.gather(
                                *[
                                    asyncio.create_task(
                                        self.fetch(target["puuid"], start, session)
                                    )
                                    for start in starts
                                ]
                            )
                        )
                        if start
                    ]
        except Exception as err:
            self.logging.error("FULL: %s", err)

    async def update_single(self, target):
        """Update a single page for a user."""
        offset = 0
        new_last = None
        new_matches = []
        try:
            async with aiohttp.ClientSession(
                headers={"X-Riot-Token": self.handler.api_key}
            ) as session:
                while True:
                    while (
                        delay := (self.retry_after - datetime.now()).total_seconds()
                    ) > 0:
                        await asyncio.sleep(min(0.1, delay))
                    url = self.endpoint_url % (target["puuid"], offset)
                    try:
                        data = await self.endpoint.request(url, session)
                        if not new_last:
                            new_last = data[0]
                        if target["last_match"] in data:
                            for entry in data:
                                if entry == target["last_match"]:
                                    break
                                new_matches.append(entry)
                            return new_matches
                        else:
                            new_matches += data
                        if new_matches:
                            self.result_matchids += new_matches
                        if offset == 900:
                            return new_matches
                        offset += 100
                    except LimitBlocked as err:
                        self.retry_after = datetime.now() + timedelta(
                            seconds=err.retry_after
                        )
                    except RatelimitException as err:
                        self.logging.error("Ratelimit")
                    except Non200Exception as err:
                        self.logging.error("Others")
                    except NotFoundException:
                        self.logging.error("Not found error.")
                    except Exception as err:
                        self.logging.error(err)
        except Exception as err:
            self.logging.error(err)
        finally:
            platform, id = new_last.split("_")
            id = int(id)
            self.result_summoners.append([platform, id, target["puuid"]])

    async def worker(self):
        """Execute requests."""
        while not self.running:
            await asyncio.sleep(5)
        while True:
            if not self.running:
                await asyncio.sleep(5)
                continue
            if not self.tasks:
                await asyncio.sleep(5)
                continue
            target = self.tasks.pop()
            if not target["last_match"]:
                await self.update_full(target)
                continue
            await self.update_single(target)

    async def flush_tasks(self, matches, summoner):
        """Insert results from requests into the db."""
        try:
            async with self.handler.postgres.acquire() as connection:
                if matches or summoner:
                    self.logging.info(
                        "Flushing %s match ids and %s summoner updates.",
                        len(matches),
                        len(summoner),
                    )
                if matches:
                    splits = []
                    for match in list(set(matches)):
                        platform, id = match.split("_")
                        splits.append((platform, int(id)))
                    query = await connection.prepare(
                        """INSERT INTO %s.match (platform, match_id) VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                        """
                        % self.name,
                    )
                    await query.executemany(splits)
                if summoner:
                    summoner_cleaned = [[s[0], s[1], s[2]] for s in summoner]
                    query = await connection.prepare(
                        """UPDATE summoner
                            SET last_updated = current_timestamp,
                                last_platform = $1,
                                last_match = $2,
                                reserved_match_history = NULL
                            WHERE puuid = $3
                        """,
                    )
                    await query.executemany(summoner_cleaned)
        except Exception as err:
            self.logging.info(err)
