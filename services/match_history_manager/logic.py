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
import asyncpg
from exceptions import RatelimitException, NotFoundException, Non200Exception
from rabbit_manager_slim import RabbitManager


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

        self.active_tasks = []

        self.rabbit = RabbitManager(exchange="SUMMONER")

        self.buffered_elements = {}  # Short term buffer to keep track of currently ongoing requests

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def async_worker(self, content):
        """Create only a new call if the summoner is not yet in the db."""
        identifier, rank, wins, losses = content
        self.buffered_elements[identifier] = True
        url = self.url % identifier
        try:
            if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(delay)
            async with aiohttp.ClientSession() as session:
                response = await self.fetch(session, url)
                return response

        except (RatelimitException, NotFoundException, Non200Exception):
            return
        finally:
            self.logging.debug("Finished extended task.")
            del self.buffered_elements[identifier]

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
            if "Retry-After" in response.headers:
                delay = int(response.headers['Retry-After'])
                self.retry_after = datetime.now() + timedelta(seconds=delay)
            raise RatelimitException()
        if response.status == 404:
            raise NotFoundException()
        if response.status != 200:
            raise Non200Exception()
        return await response.json(content_type=None)

    async def init(self):
        """Override of the default init function.

        Initiate the Rankmanager object.
        """
        await self.rabbit.init()

    async def package_manager(self):
        try:
            conn = await asyncpg.connect("postgresql://postgres@postgres/raw")
            while not self.stopped:
                entries = conn.fetch('''
                    UPDATE summoner 
                    SET checkout = CURRENT_TIMESTAMP
                    WHERE summoner_id IN (
                        SELECT summoner_id
                        FROM summoner
                        WHERE (
                            checkout IS NULL
                            OR EXTRACT(MINUTE FROM (CURRENT_TIMESTAMP - checkout)) > 5)
                            AND account_id IS NULL
                        LIMIT 25
                        )
                    RETURNING summoner_id;
                ''')

                responses = await asyncio.gather(*[
                    asyncio.create_task(self.async_worker(x['summoner_id'] for x in entries))
                ])
                entries_serialized = []
                for response in responses:
                    if response:
                        entries_serialized.append(
                            "('%s', '%s')" % (response['accountId'], response['puuid'])
                        )

            self.logging.info("Exited package manager.")
            await conn.close()
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)
            raise err

    async def run(self):
        """Runner."""
        await self.init()
        manager = asyncio.create_task(self.package_manager())
        while not self.stopped:
            await asyncio.sleep(1)
        try:
            manager.cancel()
        except:
            pass
