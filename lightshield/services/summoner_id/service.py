import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp


class Platform:
    _runner = None

    def __init__(self, name, config, handler):
        self.name = name
        self.handler = handler
        self.logging = logging.getLogger("%s" % name)
        self.tasks = []  # List of summonerIds
        self.results = []  # Properly returning entries. List of [puuid, summonerId]
        self.not_found = []  # Empty returning summoner-v4
        self._worker = []  # Worker promises
        self.updater = None  # Updater promise
        self.retry_after = datetime.now()
        self.endpoint_url = (
            f"{handler.protocol}://{name.lower()}.api.riotgames.com/lol/summoner/v4/summoners/%s"
        )

    async def run(self):
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
        while not self.handler.is_shutdown:
            if len(self.results) + len(self.not_found) >= 100:
                results = self.results.copy()
                self.results = []
                not_found = self.not_found.copy()
                self.not_found = []
                await self.flush_tasks(results, not_found)

            if len(self.tasks) > 250:
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

                if len(entries) == 0:
                    await asyncio.sleep(30)
                    results = self.results.copy()
                    self.results = []
                    not_found = self.not_found.copy()
                    self.not_found = []
                    await self.flush_tasks(results, not_found)
                self.tasks += [entry["summoner_id"] for entry in entries]

    async def worker(self):
        """Execute requests."""
        target = None
        async with aiohttp.ClientSession() as session:
            while not self.handler.is_shutdown:
                await asyncio.sleep(0.1)
                if self.retry_after > datetime.now():
                    await asyncio.sleep(0.1)
                    continue
                if not self.tasks:
                    await asyncio.sleep(1)
                    continue
                if not target:
                    target = self.tasks.pop()
                url = self.endpoint_url % target

                async with session.get(url, proxy=self.handler.proxy) as response:
                    try:
                        data = await response.json()
                    except aiohttp.ContentTypeError:
                        continue
                    if response.status == 200:
                        self.results.append([data["puuid"], data["id"]])
                        target = None
                        continue
                    if response.status == 429:
                        await asyncio.sleep(0.5)
                    if response.status == 430:
                        wait_until = datetime.fromtimestamp(data["Retry-At"])
                        seconds = (wait_until - datetime.now()).total_seconds()
                        self.retry_after = wait_until
                        seconds = max(0.1, seconds)
                        await asyncio.sleep(seconds)

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
