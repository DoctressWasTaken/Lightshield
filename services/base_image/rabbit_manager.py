import asyncio
import datetime
import logging
import os
import pickle

import aio_pika
import aiohttp
from aio_pika import ExchangeType, Message, DeliveryMode


class RabbitManager:
    def __init__(self, incoming=None, exchange="", outgoing=()):
        self.logging = logging.getLogger("RabbitMQ")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(asctime)s [RabbitMQ] %(message)s"))
        self.logging.addHandler(handler)

        self.server = os.environ["SERVER"]
        if "STREAM" in os.environ:
            self.streamID = os.environ["STREAM"]
        if "MAX_TASK_BUFFER" in os.environ:
            self.max_buffer = int(os.environ["MAX_TASK_BUFFER"])

        self.outgoing = outgoing

        self.incoming = incoming
        self.exchange = self.server + "_" + exchange

        self.queue = None
        self.blocked = True
        self.stopped = False
        self.connection = None
        self.empty_response = None

        self.fill_task = None

    def shutdown(self) -> None:
        self.stopped = True

    async def connect(self):
        self.connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop()
        )

    async def init(self, prefetch=50):
        self.queue = asyncio.Queue(maxsize=prefetch)
        self.empty_response = datetime.datetime.now()
        await self.connect()
        headers = {"content-type": "application/json"}
        while not self.stopped:
            async with aiohttp.ClientSession(
                    auth=aiohttp.BasicAuth("guest", "guest")
            ) as session:
                async with session.get(
                        "http://rabbitmq:15672/api/queues", headers=headers
                ) as response:
                    resp = await response.json()
                    queues = {entry["name"]: entry for entry in resp}
                    missing = False
                    for queue in self.outgoing:
                        if self.server + "_" + queue not in queues:
                            self.logging.info(
                                "Queue %s not initialized yet. Waiting.", queue
                            )
                            missing = True
                            break
            await asyncio.sleep(1)
            if not missing:
                return

    async def check_full(self) -> None:
        """Check if the size of any of the queues is above wanted levels."""
        headers = {"content-type": "application/json"}
        self.logging.info("Initiating buffer checker. Max buffer: %s", self.max_buffer)
        async with aiohttp.ClientSession(
                auth=aiohttp.BasicAuth("guest", "guest")
        ) as session:
            while not self.stopped:
                async with session.get(
                        "http://rabbitmq:15672/api/queues", headers=headers
                ) as response:
                    resp = await response.json()
                    queues = {entry["name"]: entry["messages"] for entry in resp}
                    was_blocked = self.blocked
                    self.blocked = False
                    queue_identifier = self.server.upper() + "_" + self.streamID
                    for queue in queues:
                        if queue.startswith(queue_identifier):
                            try:
                                if int(queues[queue]) > self.max_buffer:
                                    if not was_blocked:
                                        self.logging.info(
                                            "Queue %s is too full [%s/%s]",
                                            queue,
                                            queues[queue],
                                            self.max_buffer,
                                        )
                                    self.blocked = True

                            except Exception as err:
                                self.logging.info(err)
                                raise err
                    if was_blocked and not self.blocked:
                        self.logging.info("Blocker released.")
                await asyncio.sleep(1)

    async def fill_queue(self):
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=50)
        queue = await channel.declare_queue(
            name=self.server + "_" + self.incoming, durable=True, robust=True
        )
        while not self.stopped:
            try:
                task = await queue.get(timeout=3)
                await self.queue.put(task)
            except Exception as err:
                self.logging.info("Received %s: %s", err.__class__.__name__, err)
                await channel.close()
                channel = await self.connection.channel()
                await channel.set_qos(prefetch_count=50)
                queue = await channel.declare_queue(
                    name=self.server + "_" + self.incoming, durable=True, robust=True
                )

    async def get(self):
        return await self.queue.get()

    async def add_task(self, message) -> None:
        channel = await self.connection.channel()
        exchange = await channel.declare_exchange(
            name=self.exchange, durable=True, type=ExchangeType.TOPIC, robust=True
        )
        try:
            await exchange.publish(
                Message(
                    body=pickle.dumps(message), delivery_mode=DeliveryMode.PERSISTENT
                ),
                routing_key="",
            )
        finally:
            await channel.close()
