import asyncio
import os
import logging
from datetime import datetime, timedelta
import aiohttp
from pika_connector import Pika
from redis_connector import Redis
import json


class RatelimitException(Exception):
    """On 429 or 430 Response."""
    pass


class NotFoundException(Exception):
    """On 404-Response."""
    pass


class Non200Exception(Exception):
    """On Non-200 Response thats not 429, 430 or 404."""
    pass


class Worker:

    def __init__(self, buffer, url, identifier):
        """Initiate logging as well as pika and redis connector."""
        self.logging = logging.getLogger("Worker")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [WORKER] %(message)s'))
        self.logging.addHandler(ch)

        self.max_buffer = buffer
        self.url_template = url
        self.identifier = identifier
        self.retry_after = datetime.now()
        self.buffered_elements = {}
        self.server = os.environ['SERVER']

        self.pika = Pika()
        self.redis = Redis()

    async def main(self):
        """Run method. Called externally to initiate the worker."""
        rabbit = await self.pika.connect()  # Establish connection
        # Initiate Channel and Exchanges
        await self.initiate_pika(connection=rabbit)
        # Start runner
        await self.runner()

    async def runner(self):
        """Manage starting new worker tasks."""
        while True:
            tasks = []
            async with aiohttp.ClientSession() as session:

                while self.retry_after < datetime.now() and len(tasks) < 5000:
                    while len(self.buffered_elements) >= self.max_buffer:
                        await asyncio.sleep(0.1)

                    identifier, msg = await self.next_task()
                    tasks.append(asyncio.create_task(self.worker(
                        session,
                        identifier,
                        msg
                    )))
                    await asyncio.sleep(1 / self.max_buffer)
                if len(tasks) > 0:
                    self.logging.info(f"Flushing {len(tasks)} tasks.")
                    await asyncio.gather(*tasks)
            if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(delay)

    async def next_task(self):
        while True:
            while not (msg := await self.pika.get()):
                await asyncio.sleep(0.5)

            content = json.loads(msg.body.decode('utf-8'))
            identifier = content[self.identifier]
            if identifier in self.buffered_elements:  # Skip any further tasks for already queued
                await self.pika.ack(msg)
                continue

            if await self.is_valid(identifier, content, msg):
                self.buffered_elements[identifier] = True
                return identifier, msg
            continue


    async def fetch(self, session, url):
        async with session.get(url, proxy="http://proxy:8000") as response:
            try:
                resp = await response.json(content_type=None)
            except:
                pass
        if response.status in [429, 430]:
            if "Retry-After" in response.headers:
                delay = int(response.headers['Retry-After'])
                self.retry_after = datetime.now() + timedelta(seconds=delay)
            raise RatelimitException()
        if response.status == 404:
            raise NotFoundException()
        if response.status != 200:
            raise Non200Exception()
        return resp

    async def initiate_pika(self, connection):
        """Abstract placeholder.

        Initiate channel and exchanges in the pika connector.
        """
        pass

    async def is_valid(self, identifier, content, msg):
        """Abstract placeholder.

        Method to decide if a msg is a valid call target or should be dropped.
        """
        pass

    async def worker(self, session, identifier, msg):
        """Abstract placeholder.

        Contains calculation """
        pass

