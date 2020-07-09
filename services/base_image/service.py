"""Default worker module containing the Service class."""
import asyncio
import os
import json
import logging

from datetime import datetime, timedelta
from exceptions import RatelimitException, NotFoundException, Non200Exception

import aiohttp
import aioredis


class ServiceClass:  # pylint: disable=R0902
    """Service base class.

    Contains basic setup for the individual services using it as base class.
    """

    def __init__(self, url_snippet, marker=None, max_local_buffer=100):
        """Initiate logging and relevant variables.

        ::param url_snippet: Url part used to make calls against the API.
        Generic url part is included in the base class.
        ::param queues_out: List of queues that the service will check for size. If any of the
        queues goes above the set length the service pauses.
        """
        self.logging = logging.getLogger("Worker")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Logic] %(message)s'))
        self.logging.addHandler(handler)
        self.parallel_worker = int(os.environ['WORKER'])

        self.server = os.environ['SERVER']
        self.marker = marker
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + url_snippet

        self.redisc = None

        # Local redis outgoing buffer
        self.max_local_buffer = max_local_buffer
        self.local_buffer_full = True
        self.stopped = False

        self.retry_after = datetime.now()

        self.buffered_elements = {}

    def shutdown(self):
        """Set stopping flag.

        Handler called by the sigterm signal.
        """
        self.logging.info("Received shutdown signal.")
        self.stopped = True

    async def init(self):
        """Initiate service.

        Abstract method replaced by the service.
        """
        self.logging.info("Default async init. Service did not overwrite.")

    async def run(self, Worker):  # pylint: disable=C0103
        """Start the main service loop.

        ::param Worker: Worker class that has been inherited from the basic WorkerClass.
        """
        self.redisc = await aioredis.create_redis_pool(
            ('redis', 6379), db=0, encoding='utf-8')

        await self.init()

        workers = [Worker(self) for i in range(self.parallel_worker)]

        await asyncio.gather(
            self.check_local_buffer(),
            *[worker.run() for worker in workers]
        )

    async def get_task(self):
        """Return a task pulled from redis.

        :return redis task or None.
        """
        if task := await self.redisc.lpop('tasks'):
            task = json.loads(task)
        return task

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

    async def add_package(self, package):
        """Add a new outgoing package to the local buffer.

        :param package: A dict to be stored in redis (as stringified json).
        :return the size of the local buffer.
        """
        return await self.redisc.lpush('packages', json.dumps(package))

    async def check_local_buffer(self) -> None:
        """Set local_buffer_full flag to pause local calls."""
        count = 0
        while not self.stopped:
            count += 1
            if await self.redisc.llen('packages') > self.max_local_buffer:
                self.local_buffer_full = True
            else:
                self.local_buffer_full = False
            await asyncio.sleep(1)
            if count == 60:
                self.logging.info("Incoming Queue: %s, Outgoing Queue: %s/%s",
                                  await self.redisc.llen('tasks'),
                                  await self.redisc.llen('packages'),
                                  self.max_local_buffer)
                count = 0
