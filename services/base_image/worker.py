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

class NoMessageException(Exception):
    """Timeout exception if no message is found."""
    pass

class Worker:

    def __init__(self, buffer, url, identifier, *args, **kwargs):
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
        while True:
            await self.runner()

    async def runner(self):
        """Manage starting new worker tasks."""
        tasks = []
        start = datetime.now()
        async with aiohttp.ClientSession() as session:
            while self.retry_after < datetime.now() and len(tasks) < 5000:
                while len(self.buffered_elements) >= self.max_buffer:
                    await asyncio.sleep(0.1)

                try:
                    identifier, msg, additional_args = await self.next_task()
                except NoMessageException:
                    break
                tasks.append(asyncio.create_task(self.worker(
                    session=session,
                    identifier=identifier,
                    msg=msg,
                    **additional_args
                )))
                #await asyncio.sleep(0.01)
            if len(tasks) > 0:
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(5)
        end = datetime.now()
        self.logging.info(f"Flushed {len(tasks)} tasks. {round(len(tasks) / (end - start).total_seconds(), 2)} task/s.")
        if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
            await asyncio.sleep(delay)

    async def next_task(self):
        timeout = 0
        while True:
            while not (msg := await self.pika.get()):
                if (timeout := timeout + 1) > 20:
                    raise NoMessageException()
                await asyncio.sleep(0.5)

            if self.identifier:
                content = json.loads(msg.body.decode('utf-8'))
                identifier = content[self.identifier]
            else:
                content = identifier = msg.body.decode('utf-8')

            if identifier in self.buffered_elements:  # Skip any further tasks for already queued
                await self.pika.ack(msg)
                continue

            if additional_args := await self.is_valid(identifier, content, msg):
                self.buffered_elements[identifier] = True
                return identifier, msg, additional_args
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

    async def process(self, session, identifier, msg, **kwargs):
        """Abstract placeholder.

        Contains calculation """
        pass

    async def finalize(self, data):
        """Abstract placeholder.

        Called after the tasks have been awaited with the tasks responses as list.
        """
        pass
