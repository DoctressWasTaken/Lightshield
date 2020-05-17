import aio_pika
import asyncio
import os
import aioredis
import aiohttp
from aio_pika import Message
import json


class Worker:

    def __init__(self):
        self.server = server = os.environ['SERVER']
        self.rabbit_connection = None
        self.rabbit_exchange = None
        self.rabbit_queue = None
        self.base_url = "http://proxy:8000/match/v4/matches/%s"
        self.api_blocked = False

    ## Rabbitmq
    async def connect_rabbit(self):
        """Create a connection to rabbitmq."""
        time = 0.5
        while not self.rabbit_connection or self.rabbit.is_closed:
            self.rabbit = await aio_pika.connect(
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
        return await self.rabbit_queue.get(timeout=1, fail=False)

    async def push_task(self, data):
        """Add a task to the outgoing exchange."""
        await self.connect_rabbit()
        await self.rabbit_exchange.publish(
            Message(json.dumps(data)), 'MATCH')

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
                print("Connection to redis could not be established.")

    async def check_exists(self, matchId):
        """Check if a redis entry for this id exists."""
        await self.connect_redis()
        return await self.redis.sismember('matches', str(matchId))

    async def add_element(self, matchId):
        """Add a new matchId to redis."""
        await self.connect_redis()
        await self.redis.sadd('matches', str(matchId))

    async def process_404(self, msg, matchId):
        """Process a 404 response.

        No data is sent further. The msg is acknowledged and the ID is added.
        The ID is added despite the possibility that a match might later
        be played on that ID, however better to avoid repulling the 404.
        """
        await self.add_element(matchId)
        await self.connect_rabbit()
        await msg.ack()

    async def process_200(self, msg, matchId, data):
        """Process a 200 response.

        Data is sent further through exchange. The ID is added and the message
        is acknowledged.
        """
        await self.add_element(matchId)
        await self.push_task(data)
        await msg.ack()

    async def process(self, msg, matchId):
        """Process a message."""
        url = self.base_url % (str(matchId))
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url) as response:
                        data = await response.json(content_type=None)
                        status = response.status
                        headers = response.headers
                        if status == 404:  # Drop non-existent matches
                            await self.process_404(msg, matchId)
                            return
                        if status == 428:
                            retry_after = int(headers['retry_after'])
                            if not self.api_blocked:
                                self.api_blocked = retry_after
                            await asyncio.sleep(retry_after + 0.5)
                        elif status != 200:
                            await asyncio.sleep(0.5)
                        else:
                            await self.process_200(msg, matchId, data)
                except:
                    await asyncio.sleep(5)

    async def run(self):

        while os.environ['STATUS'] == "RUN":
            tasks = []
            current = []
            while len(tasks) < 2000 and os.environ['STATUS'] == "RUN":
                if self.api_blocked:
                    self.api_blocked += 20
                    await asyncio.sleep(self.api_blocked - 20)
                    self.api_blocked = None

                msg = await self.retrieve_task()
                matchId = int(msg.body.decode('utf-8'))
                if await self.check_exists(matchId):
                    continue  # Skip existing matchIds
                if matchId in current:
                    continue  # Skip currently called matchIds
                tasks.append(asyncio.create_task(
                    self.process(msg, matchId)
                ))
                await asyncio.sleep(0.01)

            await asyncio.gather(*tasks)
