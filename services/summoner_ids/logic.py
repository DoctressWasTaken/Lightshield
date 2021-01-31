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
import aio_pika
import traceback

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
        self.marker = RepeatMarker()
        self.retry_after = datetime.now()

        self.active_task_count = 0
        self.active_tasks = []

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

    async def task_selector(self, message):
        self.logging.debug("Started task.")
        async with message.process():
            try:
                identifier, rank, wins, losses = content = pickle.loads(message.body)
                if data := await self.marker.execute_read(
                        'SELECT accountId, puuid FROM summoner_ids WHERE summonerId = "%s";' % identifier):
                    # Pass on package directly if IDs already aquired
                    self.logging.debug("Already existent skipping.")
                    package = {'accountId': data[0][0], 'puuid': data[0][1]}

                    await self.rabbit.add_task([
                        package['accountId'],
                        package['puuid'],
                        rank,
                        wins,
                        losses
                    ])
                elif identifier not in self.buffered_elements:
                    # Create request task if it is not currently run already
                    self.logging.debug("Creating extended task.")
                    self.active_tasks.append(
                        asyncio.create_task(self.async_worker(content))
                    )
                    return
                # Case: data not already aquired but currently in progress
                # Discards task
                self.logging.debug("Discarding task.")
                self.active_task_count -= 1
            except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)
            raise err

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
            await self.marker.execute_write(
                'REPLACE INTO summoner_ids (summonerId, accountId, puuid) '
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
            return
        finally:
            self.logging.debug("Finished extended task.")
            del self.buffered_elements[identifier]
            self.active_task_count -= 1


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
        await self.marker.connect()
        await self.rabbit.init()

    async def package_manager(self):
        self.logging.info("Starting package manager.")
        connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop()
        )
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=50)
        queue = await channel.declare_queue(
            name=self.server + "_RANKED_TO_SUMMONER",
            durable=True,
            robust=True
        )
        self.logging.info("Initialized package manager.")
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                self.active_task_count += 1
                await self.task_selector(message)

                while self.active_task_count >= 25 or self.rabbit.blocked:
                    if self.active_tasks:
                        await asyncio.gather(*self.active_tasks)
                        self.active_tasks = []
                    await asyncio.sleep(0.5)

        self.logging.info("Exited package manager.")

    async def run(self):
        """Runner."""
        await self.init()
        check_task = asyncio.create_task(self.rabbit.check_full())
        manager = asyncio.create_task(self.package_manager())
        while not self.stopped:
            await asyncio.sleep(1)
        try:
            manager.cancel()
        except:
            pass
        await check_task
