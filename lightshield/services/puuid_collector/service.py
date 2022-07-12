import asyncio
import logging
from datetime import datetime

import aiohttp
from lightshield.services.puuid_collector import queries


class Platform:
    _runner = None

    def __init__(self, platform, handler):
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger("%s" % platform)
        # Todos
        self.tasks = []  # List of summonerIds
        # Dones
        self.results = []  # Properly returning entries. List of [puuid, summonerId]
        self.not_found = []  # Empty returning summoner-v4

        self.retry_after = datetime.now()
        self.endpoint_url = f"{handler.protocol}://{platform.lower()}.api.riotgames.com/lol/summoner/v4/summoners/%s"

    async def run(self):
        """Main object loop."""
        workers = 10
        self.logging.info("Start")
        while not self.handler.is_shutdown:
            async with self.handler.db.acquire() as connection:
                self.logging.info("Established Connection")
                self.results = []
                self.not_found = []
                query = queries.tasks[self.handler.connection.type].format(
                    platform=self.platform,
                    platform_lower=self.platform.lower(),
                    schema=self.handler.connection.schema
                )
                self.logging.info("Finished task query")
                try:
                    self.tasks = await connection.fetch(query, workers * 20)
                except:
                    self.logging.warning("Error is here")
                    raise
                if not self.tasks:
                    await asyncio.sleep(5)
                    workers = max(workers - 1, 1)
                    continue
                workers = len(self.tasks) // 20
                self.logging.info("Starting workers.")
                async with aiohttp.ClientSession() as session:
                    await asyncio.gather(
                        *[
                            asyncio.create_task(self.worker(session))
                            for _ in range(workers)
                        ]
                    )
                self.logging.info("Done with workers.")
                await self.flush_tasks(connection=connection)
                self.logging.info("Done with flushing.")
                workers = min(10, workers + 1)

    async def worker(self, session):
        """Execute requests."""
        target = None
        while not self.handler.is_shutdown:
            if self.retry_after > datetime.now():
                await asyncio.sleep(0.1)
                continue
            if not target:
                if not self.tasks:
                    return
                target = self.tasks.pop()
            url = self.endpoint_url % target["summoner_id"]
            try:
                async with session.get(url, proxy=self.handler.proxy) as response:
                    data = await response.json()
                match response.status:
                    case 200:
                        self.results.append(
                            [data["id"], data["puuid"], data["name"], data["revisionDate"]]
                        )
                        target = None
                    case 404:
                        self.not_found.append(target["summoner_id"])
                        target = None
                    case 429:
                        await asyncio.sleep(0.5)
                    case 430:
                        wait_until = datetime.fromtimestamp(data["Retry-At"])
                        seconds = (wait_until - datetime.now()).total_seconds()
                        self.retry_after = wait_until
                        seconds = max(0.1, seconds)
                        await asyncio.sleep(seconds)
                    case _:
                        # Other response error
                        continue
            except aiohttp.ContentTypeError:
                self.logging.error("Response was not a json.")
                continue
            except aiohttp.ClientProxyConnectionError:
                self.logging.error("Lost connection to proxy.")
                continue

    async def flush_tasks(self, connection):
        """Insert results from requests into the db."""
        if self.results or self.not_found:
            self.logging.info(
                "Flushing %s successful and %s unsuccessful finds.",
                len(self.results),
                len(self.not_found),
            )
        if self.results:
            prep = await connection.prepare(
                queries.update_ranking[self.handler.connection.type].format(
                    platform=self.platform,
                    platform_lower=self.platform.lower(),
                    schema=self.handler.connection.schema
                )
            )
            await prep.executemany([res[:2] for res in self.results])
            # update summoner Table
            converted_results = [
                [res[1], res[2], datetime.fromtimestamp(res[3] / 1000), self.platform]
                for res in self.results
            ]
            prep = await connection.prepare(
                queries.insert_summoner[self.handler.connection.type].format(
                    platform=self.platform,
                    platform_lower=self.platform.lower(),
                    schema=self.handler.connection.schema
                ))
            await prep.executemany(converted_results)

        if self.not_found:
            await connection.execute(
                queries.missing_summoner[self.handler.connection.type].format(
                    platform=self.platform,
                    platform_lower=self.platform.lower(),
                    schema=self.handler.connection.schema
                ),
                self.not_found,
            )
