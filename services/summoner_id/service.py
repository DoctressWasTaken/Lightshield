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

    def __init__(self, name, handler):
        self.name = name
        self.handler = handler
        self.logging = logging.getLogger("%s" % name)
        self.tasks = []  # List of summonerIds
        self.results = []  # Properly returning entries. List of [puuid, summonerId]
        self.not_found = []  # Empty returning summoner-v4
        self._worker = []  # Worker promises
        self.updater = None  # Updater promise
        self.retry_after = datetime.now()
        self.proxy = handler.proxy
        self.endpoint_url = (
            f"https://{name}.api.riotgames.com/lol/summoner/v4/summoners/%s"
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
            server=self.name, zone="summoner-v4"
        )
        self._runner = asyncio.create_task(self.runner())

    async def runner(self):
        """Main object loop."""
        self.updater = asyncio.create_task(self.task_updater())
        self._worker = [asyncio.create_task(self.worker()) for _ in range(10)]
        try:
            await asyncio.gather(*self._worker, self.updater)
        except asyncio.CancelledError:
            await self.flush_tasks(self.results, self.not_found)
            return

    async def task_updater(self):
        """Pull new tasks when the list is empty."""
        while True:
            if len(self.results) + len(self.not_found) >= 100:
                self.logging.debug("Flushing")
                results = self.results.copy()
                self.results = []
                not_found = self.not_found.copy()
                self.not_found = []
                await self.flush_tasks(results, not_found)
            if not self.running:
                await asyncio.sleep(5)
                continue

            if len(self.tasks) > 250:
                self.logging.debug("Too many tasks remaining, sleeping")
                await asyncio.sleep(5)
                continue
            async with self.handler.postgres.acquire() as connection:
                entries = await connection.fetch(
                    """UPDATE %s.ranking
                            SET reserved_until = CURRENT_DATE + INTERVAL '10 minute'
                            WHERE summoner_id IN (
                                SELECT summoner_id
                                FROM %s.ranking
                                WHERE NOT defunct 
                                    AND puuid IS NULL
                                    AND (reserved_until < CURRENT_TIMESTAMP OR reserved_until IS NULL)
                                    LIMIT $1
                                    FOR UPDATE
                            )
                            RETURNING summoner_id
                    """
                    % (self.name.lower(), self.name.lower()),
                    500,
                )
                self.logging.debug(
                    "Refilling tasks [%s -> %s].",
                    len(self.tasks),
                    len(self.tasks) + len(entries),
                )
                if len(entries) == 0:
                    await asyncio.sleep(30)
                    results = self.results.copy()
                    self.results = []
                    not_found = self.not_found.copy()
                    self.not_found = []
                    await self.flush_tasks(results, not_found)
                self.tasks += [entry["summoner_id"] for entry in entries]

    async def fetch(self, target, session):
        """Call and handle response."""
        url = self.endpoint_url % target
        try:
            data = await self.endpoint.request(url, session)
            self.results.append([data["puuid"], data["id"]])
            # self.logging.debug(url)
        except LimitBlocked as err:
            self.retry_after = datetime.now() + timedelta(seconds=err.retry_after)
            return target
        except RatelimitException as err:
            self.logging.error("Ratelimit")
            return target
        except Non200Exception as err:
            self.logging.error("Others")
            return target
        except NotFoundException:
            self.not_found.append(target)
        except Exception as err:
            self.logging.exception("General exception during fetch.")
            return target

    async def worker(self):
        """Execute requests."""
        targets = []
        while not self.running:
            await asyncio.sleep(5)
        while True:
            await asyncio.sleep(0.1)
            while (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(min(0.1, delay))
            if not self.running:
                await asyncio.sleep(5)
                continue
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

    async def flush_tasks(self, results, not_found):
        """Insert results from requests into the db."""
        async with self.handler.postgres.acquire() as connection:
            if results or not_found:
                self.logging.info(
                    "Flushing %s successful and %s unsuccessful finds.",
                    len(results),
                    len(not_found),
                )
            if results:
                prep = await connection.prepare(
                    """UPDATE %s.ranking
                        SET puuid = $1,
                            reserved_until = NULL
                        WHERE summoner_id =  $2
                    """
                    % self.name
                )
                await prep.executemany(results)
            if not_found:
                await connection.execute(
                    """UPDATE %s.ranking
                        SET defunct=TRUE
                        WHERE summoner_id = ANY($1::varchar)
                    """
                    % self.name,
                    not_found,
                )
