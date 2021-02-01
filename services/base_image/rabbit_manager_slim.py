import asyncio
import logging
import os
import pickle
import traceback

import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from aiormq.exceptions import DeliveryError


class RabbitManager:
    """Manage outgoing package handling."""

    def __init__(self, exchange=''):
        self.logging = logging.getLogger("RabbitMQ")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [RabbitMQ] %(message)s'))
        self.logging.addHandler(handler)

        self.exchange_name = os.environ['SERVER'] + "_" + exchange
        self.blocked = False  # Set to true if the queue rejects messages due to being full.
        self.stopped = False  # Set to true once the service receives the shutdown signal

        # Rabbit elements
        self.connection = None
        self.channel = None
        self.exchange = None

        self.outstanding_messages = []  # Contains outstanding ready messages rejected by the queue
        self.check_queue_task = None  # Contains a task instance of check_queue

        try:
            # Attempt to load already backed up tasks
            self.outstanding_messages = pickle.load(open("/backup/save.p", "rb"))
            self.logging.info("Restarted service with %s outstanding tasks." % len(self.outstanding_messages))
        except:
            pass

    def shutdown(self) -> None:
        self.stopped = True
        pickle.dump(self.outstanding_messages, open("/backup/save.p", "wb+"))

    async def connect(self):
        self.connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop())
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            name=self.exchange_name,
            durable=True,
            type=ExchangeType.TOPIC,
            robust=True)

    async def init(self):
        await self.connect()

    async def check_queue(self):
        """Attempt to add backlog of tasks to full queue.

        Once all backlogged tasks are added to the queue releases blocker.
        """
        self.logging.info("Queue full. Started scaling backoff attempts.")
        timeout = 1
        while self.outstanding_messages:
            message = self.outstanding_messages.pop()
            if not await self.exchange.publish(
                    Message(body=pickle.dumps(message),
                            delivery_mode=DeliveryMode.PERSISTENT),
                    routing_key=""):
                await asyncio.sleep(timeout)
                timeout = min(30, timeout + 1)
                self.outstanding_messages.append(message)
        self.blocked = False
        self.logging.info("Queue unblocked.")

    async def add_task(self, message) -> None:
        if self.blocked:
            self.outstanding_messages.append(message)
            return
        if self.check_queue_task:
            await self.check_queue_task
            self.check_queue_task = None
        try:
            try:
                await self.exchange.publish(
                    Message(body=pickle.dumps(message),
                            delivery_mode=DeliveryMode.PERSISTENT),
                    routing_key="")
            except DeliveryError:
                self.blocked = True
                self.outstanding_messages.append(message)
                if not self.check_queue_task:
                    self.check_queue_task = asyncio.create_task(
                        self.check_queue()
                    )
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)
            raise err
