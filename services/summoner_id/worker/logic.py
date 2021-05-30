"""Summoner ID Updater  - Logical elements.

No Service defined as the service is exactly the same as the default case.
Import is done directly.
"""
import asyncio
import logging
import os
import traceback
from datetime import datetime, timedelta
import settings
import aiohttp
from connection_manager.buffer import RedisConnector
from connection_manager.persistent import PostgresConnector
from exceptions import RatelimitException, NotFoundException, Non200Exception


class Service:
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("SummonerIDs")
        level = logging.INFO
        if settings.DEBUG:
            level = logging.DEBUG
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("%(asctime)s [SummonerIDs] %(message)s"))
        self.logging.addHandler(handler)
        self.url = (
            f"http://{settings.SERVER}.api.riotgames.com/lol/"
            + "summoner/v4/summoners/%s"
        )
        self.stopped = False
        self.retry_after = datetime.now()
        self.redis = RedisConnector()
        self.db = PostgresConnector(user=settings.SERVER)
        self.db.set_prepare(self.prepare)
        self.completed_tasks = []
        self.to_delete = []

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def insert(self, batch):
        """Update entry in postgres."""
        async with self.db.get_connection(exclusive=True):
            await self.prep_insert.executemany(batch)

    async def drop(self, summoner_id):
        async with self.db.get_connection(exclusive=True) as db:
            await db.execute(
                """
                    DELETE FROM %s.summoner
                    WHERE summoner_id = $1;
                    """,
                summoner_id,
            )

    async def get_task(self):
        """Return tasks to the async worker."""
        async with self.redis.get_connection() as buffer:
            task = await buffer.spop("%s_summoner_id_tasks" % settings.SERVER)
            if not task or self.stopped:
                return
            start = int(datetime.utcnow().timestamp())
            await buffer.zadd(
                "%s_summoner_id_in_progress" % settings.SERVER, start, task
            )
            return task

    async def logic(self, session, summoner_id):
        failed = summoner_id
        while failed:
            if datetime.now() < self.retry_after:
                delay = max(0.5, (self.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                summoner_id = failed
                failed = None
                response = await self.fetch(session, self.url % summoner_id)
                return [response["accountId"], response["puuid"], summoner_id]
            except NotFoundException:
                await self.drop(summoner_id)
            except (RatelimitException, Non200Exception):
                failed = summoner_id
                continue
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                self.logging.info(err)
            await asyncio.sleep(0.01)

    async def async_worker(self):
        """Create only a new call if the summoner is not yet in the db."""
        self.logging.info("Initiated.")
        failed = None
        while not self.stopped:
            batch = []
            async with aiohttp.ClientSession() as session:
                for _ in range(50):
                    summoner_id = await self.get_task()
                    if not summoner_id:
                        break
                    else:
                        batch.append(
                            asyncio.create_task(self.logic(session, summoner_id))
                        )
                if not batch:
                    await asyncio.sleep(60)
                    continue
                batch_results = await asyncio.gather(*batch)
            self.logging.info("Inserted %s summoner_ids.", len(batch))
            await self.insert(batch_results)

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
            async with session.get(url, proxy=settings.PROXY_URL) as response:
                await response.text()
        except aiohttp.ClientConnectionError:
            raise Non200Exception()
        if response.status in [429, 430]:
            if response.status == 430:
                if "Retry-At" in response.headers:
                    self.retry_after = datetime.strptime(
                        response.headers["Retry-At"], "%Y-%m-%d %H:%M:%S"
                    )
            elif response.status == 429:
                self.logging.info(response.status)
                delay = 1
                if "Retry-After" in response.headers:
                    delay = int(response.headers["Retry-After"])
                self.retry_after = datetime.now() + timedelta(seconds=delay)
            raise RatelimitException()
        if response.status == 404:
            raise NotFoundException()
        if response.status != 200:
            raise Non200Exception()
        return await response.json(content_type=None)

    async def prepare(self, connection):
        self.prep_insert = await connection.prepare(
            """
            UPDATE %s.summoner
            SET account_id = $1, puuid = $2
            WHERE summoner_id = $3;
            """
            % settings.SERVER
        )
        self.prep_drop = await connection.prepare(
            """
                    DELETE FROM %s.summoner
                    WHERE summoner_id = $1;
                    """
            % settings.SERVER
        )

    async def run(self):
        """Runner."""
        await self.async_worker()
