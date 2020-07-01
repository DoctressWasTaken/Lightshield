import asyncio
import signal
import os
import logging
from datetime import datetime, timedelta
import aiohttp
import aio_pika
import aioredis
import json

from exceptions import (
    RatelimitException,
    NotFoundException,
    Non200Exception,
    NoMessageException
)

class WorkerClass:

    def __init__(self, service):

        self.service = service
        self.logging = service.logging
        self.channel = None

    async def init(self):
        """Initiate worker.

        Abstract method replaced by the worker.
        """

    async def get_task(self):
        """Get Task.

        Abstract method. Returns pseudo message.
        """
        await asyncio.sleep(0.5)
        return "Hello World"

    async def process_task(self, task):
        """Process Task.

        Abstract method. Processes the received task.
        """
        await asyncio.sleep(0.5)
        self.logging.info(f"Received {task}.")

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
        async with session.get(url, proxy="http://proxy:8000") as response:
            resp = await response.text()
        if response.status in [429, 430]:
            if "Retry-After" in response.headers:
                delay = int(response.headers['Retry-After'])
                self.service.retry_after = datetime.now() + timedelta(seconds=delay)
            raise RatelimitException()
        if response.status == 404:
            raise NotFoundException()
        if response.status != 200:
            raise Non200Exception()
        return await response.json(content_type=None)

    async def run(self, channel):
        """Handle the core worker loop.

        Waits for task received, processes task. Interrupts if the outgoing queue is blocked or the
        API returns a ratelimit issue.
        """
        self.channel = channel
        await self.channel.set_qos(prefetch_count=5)
        while not self.service.stopping:
            if (delay := (self.service.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(delay)
            while self.service.queue_out_blocked:
                await asyncio.sleep(0.1)

            if task := await self.get_task():
                await self.process_task(task)


class ServiceClass:

    def __init__(self, url_snippet, queues_out):

        self.logging = logging.getLogger("Worker")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [WORKER] %(message)s'))
        self.logging.addHandler(ch)
        self.max_buffer = int(os.environ['BUFFER'])

        self.server = os.environ['SERVER']

        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + url_snippet

        self.rabbitc = None
        self.redisc = None
        self.queues_out = queues_out
        self.queue_out_blocked = True
        self.stopping = False

        self.retry_after = datetime.now()

        self.buffered_elements = {}

    def shutdown(self):
        """Set stopping flag.

        Handler called by the sigterm signal.
        """
        self.stopping = True

    async def init(self):
        """Initiate service.

        Abstract method replaced by the service.
        """
        self.logging.info("Initiated service.")

    async def run(self, Worker):

        signal.signal(signal.SIGTERM, self.shutdown)

        self.rabbitc = await aio_pika.connect_robust(
            url=f'amqp://guest:guest@rabbitmq/')

        self.redisc = await aioredis.create_redis_pool(
            ('redis', 6379), db=0, encoding='utf-8')

        await self.init()

        workers = [Worker(self) for i in range(self.max_buffer)]

        await asyncio.gather(
            self.block_messaging_queue(),
            *[worker.run(await self.rabbitc.channel()) for worker in workers]
        )

    async def block_messaging_queue(self):
        """Pause the application until the message queue falls below a certain message limit.

        To avoid overloading the messaging queue services are interrupted by this method before
        being able to continue into the next cycle.
        This method blocks the service until the web-api of the rabbitmq service used returns a
        queue length below a set limit.
        """
        if not self.queues_out:
            self.queue_out_blocked = False
            return
        headers = {
            'content-type': 'application/json'
        }
        while not self.stopping:
            async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("guest", "guest")) as session:
                async with session.get('http://rabbitmq:15672/api/queues', headers=headers) as response:
                    resp = await response.json()
                    queues = {entry['name']: entry for entry in resp}
                    for queue in self.queues_out:
                        if messages := int(queues[queue % self.server]["messages"]) > 2000:
                            self.queue_out_blocked = True
                            self.logging.info(f"Awaiting messages to be reduced. [{messages}].")
                        else:
                            self.queue_out_blocked = False
                    await asyncio.sleep(5)


if __name__ == "__main__":
    service = ServiceClass(
        url_snippet="summoner/v4/summoners/%s",
        queues_out=['MATCH_HISTORY_IN_%s'])
    asyncio.run(service.run(WorkerClass))
