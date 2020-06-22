import aio_pika
import asyncio
import os

import aiohttp
import aioredis
import websockets
from aio_pika import Message, DeliveryMode
import json
import logging

class Master:

    def __init__(self, buffer):
        self.server = server = os.environ['SERVER']
        self.rabbit = None
        self.rabbit_exchange = None
        self.rabbit_queue = None
        self.redis = None

        self.url_template = f"http://{server}.api.riotgames.com/lol/match/v4/matches/%s"

        self.max_buffer = buffer
        self.failed_tasks = []
        self.retry_after = None
        self.buffered_matches = {}

        self.logging = logging.getLogger("worker")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [WORKER] %(message)s'))
        self.logging.addHandler(ch)

    ## Rabbitmq
    async def connect_rabbit(self):
        """Create a connection to rabbitmq."""
        time = 0.5
        while not self.rabbit or self.rabbit.is_closed:
            self.rabbit = await aio_pika.connect_robust(
                'amqp://guest:guest@rabbitmq/')
            channel = await self.rabbit.channel()
            await channel.set_qos(prefetch_count=1)
            # Incoming
            self.rabbit_queue = await channel.declare_queue(
                'MATCH_IN_' + self.server, durable=True)
            # Outgoing
            self.rabbit_exchange = await channel.declare_exchange(
                f'MATCH_OUT_{self.server}', type='direct',
                durable=True)
            db_in = await channel.declare_queue(
                'DB_MATCH_IN_' + self.server, durable=True)
            await db_in.bind(self.rabbit_exchange, 'MATCH')
            await asyncio.sleep(time)
            time = min(time + 0.5, 5)
            if time == 5:
                print("Connection to rabbitmq could not be established.")

    async def retrieve_task(self):
        """Return a task from rabbitmq or empty if none are available."""
        await self.connect_rabbit()
        while True:
            try:
                return await self.rabbit_queue.get(timeout=1, fail=False)
            except Exception as err:
                self.logging.error(err)
                await asyncio.sleep(0.5)

    async def push_task(self, data):
        """Add a task to the outgoing exchange."""
        await self.connect_rabbit()
        await self.rabbit_exchange.publish(
            Message(bytes(json.dumps(data), 'utf-8')), 'MATCH')

    ## Redis
    async def connect_redis(self):
        """Create a connection to redis if not connected."""
        time = 0.5
        while not self.redis or self.redis.closed:
            self.redis = await aioredis.create_redis_pool(
                "redis://redis", db=0, encoding='utf-8')
            await asyncio.sleep(time)
            time = min(time + 0.5, 5)
            if time == 5:
                self.logging.error("Connection to redis could not be established.")

    async def check_exists(self, matchId):
        """Check if a redis entry for this id exists."""
        await self.connect_redis()
        return await self.redis.sismember('matches', str(matchId))

    async def add_element(self, matchId):
        """Add a new matchId to redis."""
        await self.connect_redis()
        await self.redis.sadd('matches', str(matchId))


    async def fetch(self, session, url, msg, matchId):
        self.logging.debug(f"Fetching {url}")
        async with session.get(url, proxy="http://proxy:8000") as response:
            try:
                resp = await response.json(content_type=None)
            except:
                pass
            if response.status in [429, 430]:
                if "Retry-After" in response.headers:
                    self.retry_after = int(response.headers['Retry-After'])
            elif response.status == 404:
                msg.reject(requeue=False)
            if response.status != 200:
                msg.reject(requeue=True)
            else:
                await self.add_element(matchId=matchId)
                await self.push_task(resp)
                await msg.ack()
            del self.buffered_matches[matchId]


    async def next_task(self):
        while True:
            msg = await self.retrieve_task()
            if not msg:
                self.logging.info("No messages found. Awaiting.")
                while not msg:
                    msg = await self.retrieve_task()
                    await asyncio.sleep(1)
            matchId = msg.body.decode('utf-8')
            if matchId in self.buffered_matches:
                self.logging.info(f"Match {matchId} is already registered as an active task.")
                try:
                    await msg.ack()
                except:
                    self.logging.info(f"Failed to ack {matchId}.")
                continue
            if await self.check_exists(matchId=matchId):
                await msg.ack()
                continue
            self.buffered_matches[matchId] = True
            return matchId, msg

    async def run(self):
        """Run method. Handles the creation and deletion of worker tasks."""
        async with aiohttp.ClientSession() as session:
            tasks = []
            while not os.environ['STATUS'] == 'STOP':
                if self.retry_after:  # Wait for retry_after timeout
                    await asyncio.sleep(self.retry_after)
                    self.retry_after = 0

                if len(self.buffered_matches) >= self.max_buffer:
                    self.logging.info("Buffer full. Waiting.")
                    while len(self.buffered_matches) >= self.max_buffer:
                        await asyncio.sleep(0.5)
                    self.logging.info("Continue")

                matchId, msg = await self.next_task()
                tasks.append(asyncio.create_task(self.fetch(
                    session=session, url=self.url_template % (matchId), msg=msg, matchId=matchId
                )))
                await asyncio.sleep(0.03)
            await asyncio.gather(*tasks)
