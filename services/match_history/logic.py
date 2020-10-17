"""Match History updater. Pulls matchlists for all player."""
import asyncio
import aiohttp
import pickle
import logging
import os
from exceptions import RatelimitException, NotFoundException, Non200Exception
from datetime import datetime, timedelta
from repeat_marker import RepeatMarker
from rabbit_manager import RabbitManager

class Service:
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("MatchHistory")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + \
                   "match/v4/matchlists/by-account/%s?beginIndex=%s&endIndex=%s&queue=420"
        self.stopped = False
        self.marker = RepeatMarker()
        self.retry_after = datetime.now()

        self.buffered_elements = {}  # Short term buffer to keep track of currently ongoing requests
        asyncio.run(self.marker.build(
            "CREATE TABLE IF NOT EXISTS match_history("
            "accountId TEXT PRIMARY KEY,"
            "matches INTEGER);"))

        self.rabbit = RabbitManager(
            exchange="HISTORY",
            incoming="SUMMONER_TO_HISTORY",
            outgoing=["HISTORY_TO_DETAILS"]
        )

        self.timelimit = int(os.environ['TIME_LIMIT'])
        self.required_matches = int(os.environ['MATCHES_TO_UPDATE'])

    async def init(self):
        """Initiate timelimit for pulled matches."""
        await self.marker.connect()
        await self.rabbit.init()

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True
        self.rabbit.shutdown()

    async def async_worker(self):
        self.logging.info("Initiated worker.")
        while not self.stopped:
            if not (task := self.rabbit.get_task()):
                await asyncio.sleep(1)
                continue
            accountId, puuid, matches, rank = pickle.loads(task.body)
            try:

                if prev := await self.marker.execute_read(
                    'SELECT matches FROM match_history WHERE accountId = "%s"' % accountId):
                    matches = matches - int(prev[0][0])
                if matches < self.required_matches:
                    continue
                if accountId in self.buffered_elements:
                    continue
                self.buffered_elements[accountId] = True

                matches_to_call = matches + 3
                calls = int(matches_to_call / 100) + 1
                ids = [start_id * 100 for start_id in range(calls)]
                calls_in_progress = []
                async with aiohttp.ClientSession() as session:
                    while ids:
                        id = ids.pop()
                        calls_in_progress.append(asyncio.create_task(
                            self.handler(
                                session=session,
                                url=self.url % (task['accountId'], id, id + 100)
                            )
                        ))
                        await asyncio.sleep(0.1)

                        responses = await asyncio.gather(*calls_in_progress)
                        match_data = list(set().union(*responses))
                        await self.marker.execute_write(
                            'UPDATE match_history SET matches = %s WHERE accountId =  "%s";' % (matches,
                                                                                                 accountId))
                        while match_data:
                            id = match_data.pop()
                            self.rabbit.add_task(id)

            except NotFoundException:
                pass
            finally:
                await task.ack()
                del self.buffered_elements[accountId]

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

    async def handler(self, session, url):
        rate_flag = False
        while not self.stopped:
            if datetime.now() < self.retry_after or rate_flag:
                rate_flag = False
                delay = max(0.5, (self.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                response = await self.fetch(session, url)
                return [match['gameId'] for match in response['matches'] if
                        match['queue'] == 420 and
                        match['platformId'] == self.server and
                        int(str(match['timestamp'])[:10]) >= self.timelimit]

            except RatelimitException:
                rate_flag = True
            except Non200Exception:
                await asyncio.sleep(0.1)

    async def run(self, rabbit):
        """Runner."""
        self.rabbit = rabbit
        await self.init()
        await asyncio.gather(*[asyncio.create_task(self.async_worker()) for _ in range(5)])
