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

    def __init__(self, buffer, url, identifier, chunksize=5000, message_out=None, **kwargs):
        """Initiate logging as well as pika and redis connector."""

        self.chunksize = chunksize
        self.message_out = message_out
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
            await self.release_messaging_queue()
            await self.runner()

    async def release_messaging_queue(self):
        """Pause the application until the message queue falls below a certain message limit."""
        headers = {
            'content-type': 'application/json'
        }
        if not self.message_out:
            return
        while True:
            async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("guest", "guest")) as session:
                async with session.get('http://rabbitmq:15672/api/queues', headers=headers) as response:
                    resp = await response.json()
                    queues = {entry['name']: entry for entry in resp}
                    messages = queues[self.message_out]["messages"]
                    if int(messages) < 10000:
                        return
                    self.logging.info(f"Awaiting messages to be reduced. [{messages}].")
                    await asyncio.sleep(5)
                    
    async def runner(self):
        """Manage starting new worker tasks."""
        tasks = []
        max_buffer_wait = 0
        start = datetime.now()
        async with aiohttp.ClientSession() as session:
            while self.retry_after < datetime.now() and len(tasks) < self.chunksize:
                while len(self.buffered_elements) >= self.max_buffer:
                    max_buffer_wait += 1
                    await asyncio.sleep(0.1)
                    if max_buffer_wait == 20:
                        self.logging.info("Waiting at max buffer.")
                max_buffer_wait = 0

                try:
                    identifier, msg, additional_args = await self.next_task()
                except NoMessageException:
                    self.logging.info("Found no message after 10 seconds. Cleaning up cycle.")
                    break
                tasks.append(asyncio.create_task(self.process(
                    session=session,
                    identifier=identifier,
                    msg=msg,
                    **additional_args
                )))
                await asyncio.sleep(0.01)
            responses = await asyncio.gather(*tasks)
        if len(tasks) > 0:
            await self.finalize(responses)
        else:
            await asyncio.sleep(5)

        end = datetime.now()
        self.logging.info(f"Flushed {len(tasks)} tasks. {round(len(tasks) / (end - start).total_seconds(), 2)} task/s.")
        if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
            await asyncio.sleep(delay)

    async def next_task(self):
        """Cycle through tasks. Returns task once found.

        Cycles until a task is found that is to be called.
        Non-eligible tasks are processed by the is_valid() function.
        Returns either a task excepts as NoMessageException if no task has been found at all after
        10 consecutive tries.
        """
        passed = 0
        while True:
            timeout = 0
            while not (msg := await self.pika.get()):
                if (timeout := timeout + 1) > 10:
                    raise NoMessageException()
                await asyncio.sleep(0.5)

            if self.identifier:
                content = json.loads(msg.body.decode('utf-8'))
                identifier = content[self.identifier]
            else:
                content = identifier = msg.body.decode('utf-8')

            if additional_args := await self.is_valid(identifier, content, msg):
                self.buffered_elements[identifier] = True
                if passed > 5:
                    self.logging.info(f"Passed {passed} elements before returning.")
                return identifier, msg, additional_args
            passed += 1

    async def fetch(self, session, url):
        async with session.get(url, proxy="http://proxy:8000") as response:
            resp = await response.text()
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
