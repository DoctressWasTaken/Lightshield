import asyncio
import threading
import os
import aiohttp
import logging
import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode


class RabbitManager:

    def __init__(self, incoming=None, exchange=None, outgoing=()):
        self.logging = logging.getLogger("LeagueRankings")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.streamID = os.environ['STREAM']
        self.max_buffer = int(os.environ['MAX_TASK_BUFFER'])

        self.outgoing = outgoing

        self.incoming = incoming
        
        self.blocked = False
        self.stopped = False
        self.connection = None
        self.exchange = self.server + "_" + exchange

    def shutdown(self) -> None:
        self.stopped = True

    async def init(self):
        self.connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop())
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=100)
        if self.incoming:
            self.incoming = await channel.declare_queue(
                name=self.server + "_" + self.incoming,
                durable=True,
                robust=True
            )
            await self.incoming.bind()
        self.exchange = await channel.declare_exchange(
            name=self.exchange,
            durable=True,
            type=ExchangeType.TOPIC,
            robust=True)
        headers = {
            'content-type': 'application/json'
        }
        while not self.stopped:
            async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("guest", "guest")) as session:
                async with session.get(
                        'http://rabbitmq:15672/api/queues', headers=headers) as response:
                    resp = await response.json()
                    queues = {entry['name']: entry for entry in resp}
                    missing = False
                    for queue in self.outgoing:
                        if self.server + "_" + queue not in queues:
                            self.logging.info("Queue %s not initialized yet. Waiting.", queue)
                            missing = True
                            break
            await asyncio.sleep(1)
            if not missing:
                return

    async def check_full(self) -> None:
        """Check if the size of any of the queues is above wanted levels."""
        headers = {
            'content-type': 'application/json'
        }
        while not self.stopped:
            async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("guest", "guest")) as session:
                async with session.get(
                        'http://rabbitmq:15672/api/queues', headers=headers) as response:
                    resp = await response.json()
                    queues = {entry['name']: entry for entry in resp}
                    self.blocked = False
                    for queue in queues:
                        if queue.startswith(self.server + self.streamID):
                            if int(queues[queue]['messages']) > self.max_buffer:
                                self.blocked = True

    async def get_task(self):
        pass

    async def add_task(self, message) -> None:

        await self.exchange.publish(
            Message(message),
            routing_key="",
            delivery_mode=DeliveryMode.PERSISTENT
        )
