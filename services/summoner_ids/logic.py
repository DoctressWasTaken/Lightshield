"""Summoner ID Updater  - Logical elements.

No Service defined as the service is exactly the same as the default case.
Import is done directly.
"""
from exceptions import RatelimitException, NotFoundException, Non200Exception
from datetime import datetime, timedelta
import asyncio
import aiohttp
import pickle
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
            logging.Formatter('%(asctime)s [SummonerIDs] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + \
                   "summoner/v4/summoners/%s"
        self.stopped = False
        self.marker = RepeatMarker()
        self.retry_after = datetime.now()

        self.rabbit = RabbitManager(
            exchange="SUMMONER",
            incoming="RANKED_TO_SUMMONER",
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
            if not (task := await self.rabbit.get_task()):
                await asyncio.sleep(1)
                continue
            identifier, rank, wins, losses = pickle.loads(task.body)

            if data := await self.marker.execute_read(
                    'SELECT accountId, puuid FROM summoner_ids WHERE summonerId = "%s";' % identifier):
                package = {'accountId': data[0][0], 'puuid': data[0][1]}

                await self.rabbit.add_task([
                    package['accountId'],
                    package['puuid'],
                    rank,
                    wins,
                    losses
                    ])
                await task.ack()

                continue
            if identifier in self.buffered_elements:
                await task.ack()
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

                await self.rabbit.add_task([
                    response['accountId'],
                    response['puuid'],
                    rank,
                    wins,
                    losses
                    ])

            except (RatelimitException, NotFoundException, Non200Exception):
                continue
            finally:
                del self.buffered_elements[identifier]
                await task.ack()

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
        rabbit_check = asyncio.create_task(self.rabbit.check_full())

        await asyncio.gather(*[asyncio.create_task(self.async_worker()) for _ in range(35)])
        await rabbit_check
