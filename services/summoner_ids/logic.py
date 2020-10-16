"""Summoner ID Updater  - Logical elements.

No Service defined as the service is exactly the same as the default case.
Import is done directly.
"""
from exceptions import RatelimitException, NotFoundException, Non200Exception
from datetime import datetime, timedelta
import asyncio
import aiohttp

import logging
import os
from repeat_marker import RepeatMarker
from rabbit_manager import RabbitManager


class Service:
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("SummonerIDs")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + \
                   "summoner/v4/summoners/%s"
        self.stopped = False
        self.marker = RepeatMarker()
        self.retry_after = datetime.now()

        self.rabbit = RabbitManager(
            exchange="SUMMONER",
            outgoing=['SUMMONER_TO_HISTORY', 'SUMMONER_TO_PROCESSOR']
        )

        self.buffered_elements = {}  # Short term buffer to keep track of currently ongoing requests
        asyncio.run(self.marker.build(
               "CREATE TABLE IF NOT EXISTS summoner_ids("
               "summonerId TEXT PRIMARY KEY,"
               "accountId TEXT,"
               "puuid TEXT);"))

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True
        self.rabbit.shutdown()

    async def async_worker(self):
        """Create only a new call if the summoner is not yet in the db."""
        while not self.stopped:
            task = await self.rabbit.get_task()
            if not task:
                await asyncio.sleep(3)
                continue
            identifier, games, rank = task.split("_")

            if data := await self.marker.execute_read(
                    'SELECT accountId, puuid FROM summoner_ids WHERE summonerId = "%s";' % identifier):
                package = {'accountId': data[0][0], 'puuid': data[0][1]}

                await self.rabbit("%s_%s_%s_%s" % (
                    package['accountId'],
                    package['puuid'],
                    games,
                    rank
                ))
                continue
            if identifier in self.buffered_elements:
                continue
            self.buffered_elements[identifier] = True
            url = self.url % identifier
            try:
                if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                    await asyncio.sleep(delay)
                async with aiohttp.ClientSession() as session:
                    response = await self.fetch(session, url)
                await self.marker.execute_write(
                    'INSERT INTO summoner_ids (summonerId, accountId, puuid) '
                    'VALUES ("%s", "%s", "%s");' % (
                    identifier, response['accountId'], response['puuid']))

                await self.rabbit("%s_%s_%s_%s" % (
                    response['accountId'],
                    response['puuid'],
                    games,
                    rank
                ))
            except (RatelimitException, NotFoundException, Non200Exception):
                continue
            finally:
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
            async with session.get(url, proxy="http://proxy:8000") as response:
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
        await self.marker.connect()
        await self.rabbit.init()

    async def run(self):
        """Runner."""
        await self.init()
        await asyncio.gather(*[asyncio.create_task(self.async_worker()) for _ in range(35)])
