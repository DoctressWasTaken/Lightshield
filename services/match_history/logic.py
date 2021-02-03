"""Match History updater. Pulls matchlists for all player."""
import asyncio
import logging
import os
import pickle
import traceback
from datetime import datetime, timedelta

import aio_pika
import aiohttp
import asyncpg
from exceptions import RatelimitException, NotFoundException, Non200Exception
from rabbit_manager_slim import RabbitManager
from repeat_marker import RepeatMarker


class Service:
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("MatchHistory")
        level = logging.INFO
        if "LOGGING" in os.environ:
            level = getattr(logging, os.environ['LOGGING'])
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
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

        self.rabbit = RabbitManager(exchange="HISTORY")

        self.timelimit = int(os.environ['TIME_LIMIT'])
        self.required_matches = int(os.environ['MATCHES_TO_UPDATE'])
        self.active_tasks = []

    async def init(self):
        """Initiate timelimit for pulled matches."""
        await self.marker.connect()
        await self.rabbit.init()

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def task_selector(self, message):
        accountId, puuid, rank, wins, losses = pickle.loads(message.body)
        matches = wins + losses
        if prev := await self.marker.execute_read(
                'SELECT matches FROM match_history WHERE accountId = "%s"' % accountId):
            matches = matches - int(prev[0][0])
        if matches < self.required_matches:
            return
        if accountId in self.buffered_elements:
            return
        self.active_tasks.append(
            asyncio.create_task(self.async_worker(accountId, matches)))

    async def async_worker(self, account_id, matches):
        self.buffered_elements[account_id] = True
        try:
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
                            url=self.url % (account_id, id, id + 100)
                        )
                    ))
                    await asyncio.sleep(0.1)
                    responses = await asyncio.gather(*calls_in_progress)
                    match_data = list(set().union(*responses))
                    query = 'REPLACE INTO match_history (accountId, matches) VALUES (\'%s\', %s);' % (
                        account_id, matches)
                    await self.marker.execute_write(query)
                    conn = await asyncpg.connect("postgresql://postgres@postgres/raw")

                    result = await conn.execute('''
                        INSERT INTO match ("matchId")
                        VALUES %s
                        ON CONFLICT ("matchId")
                        DO NOTHING;
                    ''' % ",".join(["(%s)" % matchId for matchId in match_data]))
                    await conn.close()
                    self.logging.debug(result)


        except NotFoundException:
            return
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)
        finally:
            self.logging.debug("Finished task.")
            del self.buffered_elements[account_id]

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

    async def package_manager(self):
        try:
            self.logging.info("Starting package manager.")
            connection = await aio_pika.connect_robust(
                "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop()
            )
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=50)
            queue = await channel.declare_queue(
                name=self.server + "_SUMMONER_TO_HISTORY",
                passive=True
            )
            self.logging.info("Initialized package manager.")
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        await self.task_selector(message)

                    while len(self.buffered_elements) >= 25 or self.rabbit.blocked:
                        if self.active_tasks:
                            await asyncio.gather(*self.active_tasks)
                            self.active_tasks = []
                        await asyncio.sleep(0.5)

            self.logging.info("Exited package manager.")
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

    async def run(self):
        """Runner."""
        await self.init()
        self.logging.info("Initiated.")
        manager = asyncio.create_task(self.package_manager())
        while not self.stopped:
            await asyncio.sleep(1)
        try:
            manager.cancel()
        except:
            pass
