"""League Updater Module."""
import asyncio
from datetime import datetime, timedelta
import os
import aiohttp
import logging
from repeat_marker import RepeatMarker
from rank_manager import RankManager

from exceptions import RatelimitException, NotFoundException, Non200Exception
from rabbit_manager import RabbitManager


class Service:  # pylint: disable=R0902
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("LeagueRankings")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + \
            "league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        self.rankmanager = RankManager()
        self.empty = False
        self.next_page = 1
        self.stopped = False
        self.marker = RepeatMarker()
        self.retry_after = datetime.now()

        self.rabbit = RabbitManager(
            exchange="RANKED",
            outgoing=['RANKED_TO_SUMMONER']
        )

        asyncio.run(self.marker.build(
               "CREATE TABLE IF NOT EXISTS match_history("
               "summonerId TEXT PRIMARY KEY,"
               "matches INTEGER);"))

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True
        self.rabbit.shutdown()

    async def init(self):
        """Override of the default init function.

        Initiate the Rankmanager object.
        """
        await self.marker.connect()
        await self.rankmanager.init()
        await self.rabbit.init()

    async def async_worker(self, tier, division):

        failed = None
        while (not self.empty or failed) and not self.stopped:
            if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(delay)

            while self.rabbit.blocked and not self.stopped:
                await asyncio.sleep(1)

            if self.stopped:
                return

            if not failed:
                page = self.next_page
                self.next_page += 1
            else:
                page = failed
                failed = None
            async with aiohttp.ClientSession() as session:
                try:
                    content = await self.fetch(session, url=self.url % (
                    tier, division, page))
                    if len(content) == 0:
                        self.logging.info("Page %s is empty.", page)
                        self.empty = True
                        return
                    await self.process_task(content)
                except (RatelimitException, Non200Exception):
                    failed = page
                except NotFoundException:
                    self.empty = True

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

    async def process_task(self, content) -> None:
        """Process the received list of summoner.

        Check for changes that would warrent it be sent on.
         """
        for entry in content:
            matches_local = entry['wins'] + entry['losses']
            matches = None
            if prev := await self.marker.execute_read(
                    'SELECT matches FROM match_history WHERE summonerId = "%s";' % entry['summonerId']):
                matches = int(prev[0][0])
                
            if matches and matches == matches_local:
                return
            await self.marker.execute_write(
                'UPDATE match_history SET matches = %s WHERE summonerId = "%s";' % (
                matches_local,
            entry['summonerId']))
            
            await self.rabbit.add_task(entry['summonerId'] + "_" + str(matches_local))

    async def run(self):
        """Override the default run method due to special case.

        Worker are started and stopped after each tier/rank combination.
        """
        await self.init()
        while not self.stopped:
            buffer_manager = asyncio.create_task(self.rabbit.check_full())
            tier, division = await self.rankmanager.get_next()
            self.empty = False
            self.next_page = 1
            await asyncio.gather(*[asyncio.create_task(self.async_worker(tier, division)) for i in range(5)])
            await self.rankmanager.update(key=(tier, division))
        await buffer_manager
