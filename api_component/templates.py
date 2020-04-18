import datetime
import asyncio
from aio_pika.exceptions import QueueEmpty
import json
import aio_pika
from aio_pika import DeliveryMode

class Limit:

    def __init__(self, span: int, max: int):
        print(f"\t\tInitiated Limit {max} over {span}")
        self.span = span
        self.max = max
        self.current = 0
        self.bucket_start = None
        self.last_update = datetime.datetime.now()

    def is_blocked(self):
        """Return if the limit is reached."""
        self.update_bucket()
        if self.current < self.max * 0.5:
            return False
        else:
            return True

    def add(self):
        self.update_bucket()
        self.current += 1

    def update_bucket(self):
        if not self.bucket_start:
            self.bucket_start = datetime.datetime.now()

        elif (datetime.datetime.now() - self.bucket_start).total_seconds() > self.span:
            self.bucket_start = datetime.datetime.now()
            self.current = 0


class DefaultEndpoint:

    tasks = 0
    methods = {}
    url = ""
    name = ""

    def __init__(self, limits, api):
        print(f"\tInitiating {self.__class__.__name__}")
        self.api = api
        self.limits = []
        for limit in limits:
            self.limits.append(
                Limit(span=int(limit), max=limits[limit])
            )

    async def run(self, connection):
        print(f"Listening for {self.__class__.__name__} Requests on {self.api.server}_{self.name}")
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=20)
        queue = await channel.declare_queue(
            self.api.server + "_" + self.name, durable=True)
        tasks = []
        while True:
            if self.tasks > 250:
                await asyncio.sleep(0.1)
                continue
            full = False
            for limit in self.limits:
                if limit.is_blocked():
                    full = True
            for limit in self.api.application_limits:
                if limit.is_blocked():
                    full = True
            if full:
                await asyncio.sleep(0.1)
                continue
            try:
                message = await queue.get(timeout=5)
            except QueueEmpty:
                await asyncio.sleep(5)
                continue
            data_tuple = await self.parse_message(message)
            if not data_tuple:
                await message.ack()
                continue

            for limit in self.limits:
                limit.add()
            for limit in self.api.application_limits:
                limit.add()

            tasks.append(
                asyncio.create_task(self.process_task(data_tuple, message, channel))
            )

        await asyncio.gather(*tasks)

    async def process_task(self, data_tuple, message,  channel):
        self.tasks += 1
        print(self.api.server, self.name, self.tasks)
        try:
            resp = await self.request(data_tuple)
        except Exception as err:
            print(err)
            await message.reject(requeue=True)
            self.tasks -= 1
            print(self.api.server, self.name, self.tasks)
            return
        await self.return_message(channel, resp, message.headers_raw)
        await message.ack()
        self.tasks -= 1
        print(self.api.server, self.name, self.tasks)

        return

    async def parse_message(self, message):
        msg = json.loads(message.body.decode())
        if msg['method'] not in self.methods:
            return None
        for param in self.methods[msg['method']]['params']:
            if param not in msg['params']:
                return None

        return msg, self.methods[msg['method']]

    async def request(self, data_tuple):

            data = data_tuple[0]
            method = data_tuple[1]

            param_list = []
            for param in method['params']:
                param_list.append(data['params'][param])
            url = self.url
            url += method['url'] % tuple(param_list)

            response = await self.api.send(url)

            if response[2] not in method['allowed_codes']:
                print(response[2])
                print("Not in allowed codes")
                raise Exception('Not an allowed response code')
            return response[0]

    async def return_message(self, channel, resp, headers):
        return_queue = headers['return'].decode()
        del headers['return']
        await channel.default_exchange.publish(
            aio_pika.Message(
                headers=headers,
                body=json.dumps(resp).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key=return_queue
        )
