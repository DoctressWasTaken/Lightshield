"""Summoner ID Updater  - Logical elements.

No Service defined as the service is exactly the same as the default case.
Import is done directly.
"""
import asyncio
import logging
import os
import traceback
from datetime import datetime, timedelta

import aiohttp
import aioredis
import asyncpg
from exceptions import RatelimitException, NotFoundException, Non200Exception


class Service:
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("SummonerIDs")
        level = logging.INFO
        if "LOGGING" in os.environ:
            level = getattr(logging, os.environ['LOGGING'])
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [SummonerIDs] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + \
                   "summoner/v4/summoners/%s"
        self.stopped = False
        self.retry_after = datetime.now()

        self.completed_tasks = []
        self.to_delete = []

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def flush_manager(self):
        """Update entries in postgres once enough tasks are done."""
        try:
            conn = await asyncpg.connect("postgresql://postgres@postgres/raw")
            if self.completed_tasks:
                self.logging.info("Inserting %s summoner IDs.", len(self.completed_tasks))
                await conn.executemany('''
                    UPDATE summoner
                    SET account_id = $1, puuid = $2
                    WHERE summoner_id = $3;
                    ''', self.completed_tasks)
                self.completed_tasks = []
            if self.to_delete:
                await conn.execute('''
                    DROP FROM summoner
                    WHERE summoner_id IN (%s);
                    ''', ",".join(["'%s'" % id for id in self.to_delete]))
                self.to_delete = []

            await conn.close()
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

    async def get_task(self):
        """Return tasks to the async worker."""

        task = await self.redis.spop('tasks')
        if not task or self.stopped:
            return
        start = int(datetime.utcnow().timestamp())
        await self.redis.zadd('in_progress', start, task)
        return task

    async def fetch_manager(self, session, summoner_id):
        """Manage the fetch response."""

        try:
            response = await self.fetch(session, self.url % summoner_id)
            self.completed_tasks.append(
                (response['accountId'], response['puuid'], summoner_id))
        except NotFoundException:
            self.to_delete.append(summoner_id)
        except (RatelimitException, Non200Exception):
            return

    async def async_worker(self):
        """Create only a new call if the summoner is not yet in the db."""
        self.logging.info("Started worker.")
        while not self.stopped:
            if datetime.now() < self.retry_after:
                delay = max(0.5, (self.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                tasks = []
                async with aiohttp.ClientSession() as session:
                    for i in range(50):
                        summoner_id = await self.get_task()
                        if not summoner_id:
                            continue
                        tasks.append(asyncio.create_task(
                            self.fetch_manager(session, summoner_id)
                        ))
                        await asyncio.sleep(0.02)
                    await asyncio.gather(*tasks)
                await self.flush_manager()
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                self.logging.info(err)
        self.logging.info("Stopped worker.")

    async def fetch(self, session, url):
        """Execute call to external target using the proxy server.

        Receives aiohttp session as well as url to be called. Executes the request and returns
        either the content of the response as json or raises an exeption depending on response.
        :param session: The aiohttp Clientsession used to execute the call.
        :param url: String url ready to be requested.

        :returns: Request response as dict.

        :raises RatelimitException: on 429 or 430 HTTP Code.
        :raises NotFoundException: on 404 HTTP Code.
        :raises Non200Exception: on any other non 200 HTTP Code.
        """
        try:
            async with session.get(url, proxy="http://lightshield_proxy_%s:8000" % self.server.lower()) as response:
                await response.text()
                self.logging.info(response.status)
        except aiohttp.ClientConnectionError:
            raise Non200Exception()
        if response.status in [429, 430]:
            if "Retry-After" in response.headers:
                delay = int(response.headers['Retry-After'])
                self.retry_after = datetime.now() + timedelta(seconds=delay)
        if response.status == 404:
            raise NotFoundException()
        if response.status != 200:
            raise Non200Exception()
        return await response.json(content_type=None)

    async def init(self):
        """Override of the default init function.

        Initiate the Rankmanager object.
        """
        self.redis = await aioredis.create_redis_pool(
            ('redis', 6379), encoding='utf-8')

    async def run(self):
        """Runner."""
        await self.init()
        await self.async_worker()
