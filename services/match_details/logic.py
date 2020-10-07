"""Match History updater. Pulls matchlists for all player."""
import asyncio
import aiohttp
import json
import logging
import os
from exceptions import RatelimitException, NotFoundException, Non200Exception
from datetime import datetime, timedelta
from repeat_marker import RepeatMarker
from buffer_manager import BufferManager

class Service:
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("MatchDetails")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + \
                   "match/v4/matches/%s"
        self.stopped = False
        self.marker = RepeatMarker()
        self.retry_after = datetime.now()
        self.manager = BufferManager()
        self.buffered_elements = {}  # Short term buffer to keep track of currently ongoing requests
        asyncio.run(self.marker.build(
            "CREATE TABLE IF NOT EXISTS match_id("
            "id BIGINT PRIMARY KEY);"))

    async def init(self):
        """Override of the default init function.

        Initiate the Rankmanager object.
        """
        await self.marker.connect()
        await self.manager.init()

    async def limiter(self):
        """Method to periodically break down the db size by removing a % of the lowest match Ids."""
        retain_period_days = 60
        removal = 100/retain_period_days
        while True:
            await asyncio.sleep(24*60*60)
            count = await self.marker.execute_read(
                'SELECT COUNT(*) FROM match_id'
            )
            to_delete = int(count * removal)
            lowest_limit = await self.marker.execute_read(
                'SELECT id FROM match_id ORDER BY id ASC LIMIT %s' % to_delete
            )[-1]
            await self.marker.execute_read(
                'DELETE FROM match_id WHERE id <= %s' % to_delete
            )

    async def async_worker(self):
        failed = None
        while not self.stopped:
            if not failed:
                task = await self.manager.get_task()
            else:
                task = failed
                failed = None
            if not task:
                await asyncio.sleep(3)
                continue
            identifier = task['match_id']
            if await self.marker.execute_read(
                    'SELECT * FROM match_id WHERE id = %s;' % identifier):
                return
            if identifier in self.buffered_elements:
                return
            self.buffered_elements[identifier] = True
            url = self.url % identifier
            try:
                if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                    await asyncio.sleep(delay)
                async with aiohttp.ClientSession() as session:
                    response = await self.fetch(session, url)
                    await self.marker.execute_write(
                        'INSERT OR IGNORE INTO match_id (id) VALUES (%s);' % identifier)
                    await self.manager.add_package(response)
            except (RatelimitException, Non200Exception):
                failed = task
            except NotFoundException:
                pass
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

    async def run(self):
        """Runner."""
        await self.init()
        limiter_task = asyncio.create_task(self.limiter())
        await asyncio.gather(*[asyncio.create_task(self.async_worker()) for _ in range(45)])
        await limiter_task
