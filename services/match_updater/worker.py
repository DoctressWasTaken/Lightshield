import aio_pika
import asyncio
import os
import aioredis
import websockets
from aio_pika import Message
import json


class Master:

    def __init__(self):
        self.server = server = os.environ['SERVER']
        self.rabbit = None
        self.rabbit_exchange = None
        self.rabbit_queue = None
        self.redis = None

    ## Rabbitmq
    async def connect_rabbit(self):
        """Create a connection to rabbitmq."""
        time = 0.5
        while not self.rabbit or self.rabbit.is_closed:
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
        while True:
            try:
                return await self.rabbit_queue.get(timeout=1, fail=False)
            except Exception as err:
                print(err)
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
                print("Connection to redis could not be established.")

    async def check_exists(self, matchId):
        """Check if a redis entry for this id exists."""
        await self.connect_redis()
        return await self.redis.sismember('matches', str(matchId))

    async def add_element(self, matchId):
        """Add a new matchId to redis."""
        await self.connect_redis()
        await self.redis.sadd('matches', str(matchId))

    async def prepare_tasks(self, count):
        """Process a message."""
        tasks = []
        ids = []
        try:
            while not os.environ['STATUS'] == 'STOP' and len(tasks) < count:
                msg = await self.retrieve_task()
                if not msg:
                    break
                matchId = int(msg.body.decode('utf-8'))
                if await self.check_exists(matchId):
                    await msg.ack()
                    continue
                if matchId in ids:
                    await msg.ack()
                    continue
                ids.append(matchId)
                tasks.append(msg)
            return tasks
        except Exception as err:
            print(err)

    async def process_successful(self, successfull_sets):
        """Process and send out the successfull requests."""
        for entry in successfull_sets:
            msg = entry[0]
            result = entry[1]
            matchId = int(msg.body.decode('utf-8'))
            await self.add_element(matchId)
            await self.push_task(result)
            await msg.ack()

    async def process_404(self, not_found_sets):
        """Process the 404 Responses."""
        for entry in not_found_sets:
            msg = entry[0]
            result = entry[1]
            matchId = int(msg.body.decode('utf-8'))
            await self.add_element(matchId)
            await msg.ack()

    async def run(self):
        """Run method. Handles the creation and deletion of worker tasks."""
        print("Startup")
        while not os.environ['STATUS'] == 'STOP':
            tasks = await self.prepare_tasks(400)
            if not tasks:
                print("No tasks found.")
                await asyncio.sleep(1)
                continue

            call_tasks = []
            for id, msg in enumerate(tasks):
                call_tasks.append([id, int(msg.body.decode('utf-8'))])
            message = {
                "endpoint": "match",
                "url": "/match/v4/matches/%s",
                "tasks": call_tasks
            }
            data_sets = []
            not_found = []
            print("Making calls. Total:", len(call_tasks))
            async with websockets.connect("ws://proxy:6789", max_size=None) as websocket:
                remaining = len(call_tasks)
                await websocket.send("Match_Updater")
                await websocket.send(json.dumps(message))
                while remaining > 0:
                    msg = await websocket.recv()
                    data = json.loads(msg)
                    if data['status'] == "rejected":
                        for entry in data['tasks']:
                            remaining -= 1
                            await tasks[entry['id']].reject(requeue=True)
                    elif data['status'] == 'done':
                        for entry in data['tasks']:
                            remaining -= 1
                            if entry['status'] == 200:
                                if len(entry['result']) == 0:
                                    empty = True
                                else:
                                    data_sets.append([
                                        tasks[entry['id']],
                                        entry['result']])
                            elif entry['status'] == 404:
                                not_found.append([
                                        tasks[entry['id']],
                                        entry['result']])
                            else:
                                await tasks[entry['id']].reject(requeue=True)
                                print(entry['status'])
                                print(entry['headers'])
            print("Done with calls. Stats: Successfull:", len(data_sets),
                  "Total:", len(tasks))

            await self.process_404(not_found)
            await self.process_successful(data_sets)
            await asyncio.sleep(1)
