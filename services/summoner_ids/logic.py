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

    async def insert(self, task):
        """Update entry in postgres."""
        await self.prep_insert.execute(task)

    async def drop(self, task):
        await self.prep_drop(task)

    async def get_task(self):
        """Return tasks to the async worker."""

        task = await self.redis.spop('%s_summoner_id_tasks' % self.server)
        if not task or self.stopped:
            return
        start = int(datetime.utcnow().timestamp())
        await self.redis.zadd('%s_summoner_id_in_progress' % self.server, start, task)
        return task

    async def async_worker(self, delay):
        """Create only a new call if the summoner is not yet in the db."""
        await asyncio.sleep(delay)
        self.logging.info("Started worker.")
        failed = None
        while not self.stopped:
            if datetime.now() < self.retry_after:
                delay = max(0.5, (self.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                async with aiohttp.ClientSession() as session:
                    if failed:
                        summoner_id = failed
                        failed = None
                    else:
                        summoner_id = await self.get_task()
                    if not summoner_id:
                        continue
                    response = await self.fetch(session, self.url % summoner_id)
                    async with self.conn_lock:
                        await self.insert([response['accountId'], response['puuid'], summoner_id])
            except NotFoundException:
                async with self.conn_lock:
                    await self.drop(summoner_id)
            except (RatelimitException, Non200Exception):
                failed = summoner_id
                continue
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
        except aiohttp.ClientConnectionError:
            raise Non200Exception()
        if response.status in [429, 430]:
            if response.status == 429:
                self.logging.info(response.status)
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
        self.conn = await asyncpg.connect("postgresql://%s@192.168.0.1/%s" % (self.server.lower(), self.server.lower()))
        self.prep_insert = await self.conn.prepare('''
            UPDATE summoner
            SET account_id = $1, puuid = $2
            WHERE summoner_id = $3;
            ''')
        self.prep_drop = await self.conn.prepare('''
                    DELETE FROM summoner
                    WHERE summoner_id = $1;
                    ''')
        self.conn_lock = asyncio.Lock()

    async def run(self):
        """Runner."""
        await self.init()
        await asyncio.gather(*[asyncio.create_task(self.async_worker(delay=i)) for i in range(5)])
        await self.conn.close()
