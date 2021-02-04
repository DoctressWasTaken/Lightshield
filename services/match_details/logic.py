"""Match History updater. Pulls matchlists for all player."""
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
        self.retry_after = datetime.now()
        self.rabbit = RabbitManager(exchange="DETAILS")
        self.active_tasks = 0
        self.working_tasks = []
        self.buffered_elements = {}  # Short term buffer to keep track of currently ongoing requests

    async def init(self):
        """Override of the default init function.

        Initiate the Rankmanager object.
        """
        await self.rabbit.init()

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def async_worker(self, matchId):
        try:
            url = self.url % matchId
            async with aiohttp.ClientSession() as session:
                response = await self.fetch(session, url)

                await self.rabbit.add_task(response)
                return matchId
        except (RatelimitException, Non200Exception):
            self.logging.info("Failed to receive data, requeuing task.")
        except NotFoundException:
            self.logging.info("Match not found.")
            return "!%s" % matchId
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

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

    async def package_manager(self):
        self.logging.info("Starting package manager.")

        while not self.stopped:
            conn = await asyncpg.connect("postgresql://postgres@postgres/raw")

            match_id_records = await conn.fetch('''
                SELECT "matchId"
                FROM match 
                WHERE status IS NULL 
                LIMIT 50;
                ''')

            tasks = []
            for record in match_id_records:
                tasks.append(
                    asyncio.create_task(self.async_worker(record['matchId']))
                )
                await asyncio.sleep(0.005)
            responses = await asyncio.gather(*tasks)
            success = []
            not_found = []
            for entry in responses:
                if not entry:
                    continue
                if str(entry).startswith("!"):
                    not_found.append(entry[1:])
                else:
                    success.append(entry)
            if success:
                await conn.execute('''
                    UPDATE match
                    SET status='C'
                    WHERE "matchId" in (%s)
                    ''' % ','.join(str(x) for x in success))
            if not_found:
                await conn.execute('''
                    DELETE FROM match
                    WHERE "matchId" in (%s)
                    ''' % ','.join(str(x) for x in not_found))
            await conn.close()
            if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(delay)
            while self.rabbit.blocked:
                await asyncio.sleep(0.25)

        self.logging.info("Exited package manager.")

    async def run(self):
        """Runner."""
        await self.init()
        await self.package_manager()
