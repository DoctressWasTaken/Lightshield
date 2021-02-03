"""Match History updater. Pulls matchlists for all player."""
import asyncio
import logging
import os
import pickle
import traceback
from datetime import datetime, timedelta

import aio_pika
import aiohttp
from exceptions import RatelimitException, NotFoundException, Non200Exception
from rabbit_manager_slim import RabbitManager
from repeat_marker import RepeatMarker


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
        self.rabbit = RabbitManager(exchange="DETAILS")
        self.active_tasks = 0
        self.working_tasks = []
        self.buffered_elements = {}  # Short term buffer to keep track of currently ongoing requests
        asyncio.run(self.marker.build(
            "CREATE TABLE IF NOT EXISTS match_id("
            "id BIGINT PRIMARY KEY);"))

    async def init(self):
        """Override of the default init function.

        Initiate the Rankmanager object.
        """
        await self.marker.connect()
        await self.rabbit.init()

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def limiter(self):
        """Method to periodically break down the db size by removing a % of the lowest match Ids."""
        retain_period_days = 60
        removal = 100 / retain_period_days
        await asyncio.sleep(7 * 24 * 60 * 60)  # Initial 7 day wait
        while True:
            await asyncio.sleep(24 * 60 * 60)
            count = await self.marker.execute_read(
                'SELECT COUNT(*) FROM match_id'
            )
            to_delete = int(count * removal)
            lowest_limit = await self.marker.execute_read(
                'SELECT id FROM match_id ORDER BY id ASC LIMIT %s' % to_delete
            )[-1]
            await self.marker.execute_read(
                'DELETE FROM match_id WHERE id <= %s' % lowest_limit
            )

    async def task_selector(self, message):
        try:
            async with message.process():
                matchId = pickle.loads(message.body)
                if await self.marker.execute_read(
                        'SELECT * FROM match_id WHERE id = %s;' % matchId):
                    self.active_tasks -= 1
                    return
                if matchId in self.buffered_elements:
                    self.active_tasks -= 1
                    return
                self.working_tasks.append(
                    asyncio.create_task(self.async_worker(matchId))
                )
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

    async def async_worker(self, matchId):
        try:
            self.buffered_elements[matchId] = True
            url = self.url % matchId
            if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(delay)
            async with aiohttp.ClientSession() as session:
                response = await self.fetch(session, url)
                await self.marker.execute_write(
                    'INSERT OR IGNORE INTO match_id (id) VALUES (%s);' % matchId)

                await self.rabbit.add_task(response)
            self.active_tasks -= 1

        except (RatelimitException, Non200Exception):
            self.working_tasks.append(
                asyncio.create_task(self.async_worker(matchId))
            )
        except NotFoundException:
            self.active_tasks -= 1
        finally:
            if matchId in self.buffered_elements:
                del self.buffered_elements[matchId]

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
        connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop()
        )
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=50)
        queue = await channel.declare_queue(
            name=self.server + "_HISTORY_TO_DETAILS",
            passive=True
        )
        self.logging.info("Initialized package manager.")
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                self.active_tasks += 1
                await self.task_selector(message)

                while self.active_tasks >= 50 or self.rabbit.blocked:
                    await asyncio.sleep(0.5)
                    await asyncio.gather(*self.working_tasks)
                    self.working_tasks = []

        self.logging.info("Exited package manager.")

    async def run(self):
        """Runner."""
        await self.init()
        limiter_task = asyncio.create_task(self.limiter())
        manager = asyncio.create_task(self.package_manager())
        while not self.stopped:
            await asyncio.sleep(0.5)
        try:
            manager.cancel()
        except:
            pass
        await limiter_task
