import asyncio
import logging
from datetime import datetime

import aiohttp
import asyncpg

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
        self.batchsize = 4000
        self.retry_after = datetime.now()
        self.endpoint_url = f"{handler.protocol}://{platform.lower()}.api.riotgames.com/lol/summoner/v4/summoners/%s"
        self.ratelimit_reached = False

    async def run(self):
        """Main object loop."""
        while not self.handler.is_shutdown:
            if len(self.tasks) <= 2000:
                self.tasks += await self.gather_tasks()
            if not self.tasks:
                await asyncio.sleep(5)
                continue
            semaphore = asyncio.Semaphore(20)
            async_threads = []
            async with aiohttp.ClientSession() as session:
                while self.tasks:
                    if self.ratelimit_reached:
                        break
                    async with semaphore:
                        task = self.tasks.pop()
                        async_threads.append(
                            asyncio.create_task(self.task_handler(session, task)))
            await asyncio.gather(*async_threads)
            await self.flush_tasks()
            self.batchsize = 4000 - len(self.tasks)

    async def gather_tasks(self):
        """Get tasks from db."""
        while not self.handler.is_shutdown:
            async with self.handler.db.acquire() as connection:
                self.results = []
                self.not_found = []
                query = queries.tasks[self.handler.connection.type].format(
                    platform=self.platform,
                    platform_lower=self.platform.lower(),
                    schema=self.handler.connection.schema
                )
                try:
                    return await connection.fetch(query, self.batchsize)

                except asyncpg.InternalServerError:
                    self.logging.info("Internal server error with db.")
            await asyncio.sleep(1)

    async def task_handler(self, session, task):
        """Execute requests."""
        url = self.endpoint_url % task["summoner_id"]
        try:
            async with session.get(url, proxy=self.handler.proxy) as response:
                data = await response.json()
            match response.status:
                case 200:
                    self.results.append(
                        [data["id"], data["puuid"], data["name"], data["revisionDate"]]
                    )
                    task = None
                case 404:
                    self.not_found.append(task["summoner_id"])
                    task = None
                case 429:
                    self.ratelimit_reached = True
                case 430:
                    self.ratelimit_reached = True
                    wait_until = datetime.fromtimestamp(data["Retry-At"])
                    self.retry_after = wait_until
        except aiohttp.ContentTypeError:
            self.logging.error("Response was not a json.")
        except aiohttp.ClientProxyConnectionError:
            self.logging.error("Lost connection to proxy.")
        except aiohttp.ClientOSError:
            self.logging.error("Connection reset.")
        finally:
            if task:
                self.tasks.append(task)

    async def flush_tasks(self):
        """Insert results from requests into the db."""
        async with self.handler.db.acquire() as connection:
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
