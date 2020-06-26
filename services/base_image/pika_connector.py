import asyncio
import aio_pika
import logging
import os
import json
from aio_pika import Message
from aio_pika import DeliveryMode

class Pika:

    def __init__(self, host='rabbitmq'):

        self.host = host
        self.server = os.environ['SERVER']
        self.logging = logging.getLogger("rabbitmq")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [RABBITMQ] %(message)s'))
        self.logging.addHandler(ch)
        self.rabbit = None
        self.incoming = None
        self.outgoing = None
        self.tag = None  # Outgoing exchange tag

        self.fails = 0  # To count failed requests in a row

    async def init(self, incoming, outgoing, tag):
        """Initiate in and outputs."""
        self.incoming = incoming
        self.outgoing = outgoing
        self.tag = tag

    async def connect(self):
        """Connect to rabbitmq.

        :raises:: ConnectionError if connection can't be established.
        """
        time = 0.5
        while not self.rabbit or self.rabbit.is_closed:
            self.rabbit = await aio_pika.connect_robust(
                url=f'amqp://guest:guest@{self.host}/')
            await asyncio.sleep(time)
            time = min(time + 0.5, 5)
            if time == 5:
                raise ConnectionError("Connection to rabbitmq could not be established.")
        return self.rabbit

    async def ack(self, msg):
        """Acknowledge a message."""
        for i in range(5):
            try:
                await asyncio.wait_for(msg.ack(), timeout=2)
                return
            except asyncio.TimeoutError:
                pass
            except Exception as err:
                self.logging.info(f"[Ack] Got exception {err.__class__.__name__}: {repr(err)}")
                return
        raise Exception("Failed to acknowledge message.")

    async def reject(self, msg, requeue):
        """Reject a message.

        Additional param for requeing.
        """
        for i in range(5):
            try:
                await asyncio.wait_for(msg.reject(requeue=requeue), timeout=2)
                return
            except asyncio.TimeoutError:
                pass
            except Exception as err:
                self.logging.info(f"[Reject:{requeue}] Got exception {err.__class__.__name__}: {repr(err)}")
                return
        raise Exception("Failed to requeue message.")

    async def get(self):
        """Get message from rabbitmq.

        Returns either the message element or None on timeout.
        """
        try:
            msg = await asyncio.wait_for(self.incoming.get(fail=False), timeout=2)
            self.fails = 0
            return msg
        except asyncio.TimeoutError:
            self.fails += 1
            if self.fails > 5:
                self.logging.info(f"{self.fails} fails in a row. Failed get.")
            return None

    async def push(self, data):
        """Push data through exchange.

        The data is used by both db_worker and match_history_updater.
        """
        if type(data) in [dict, list]:
            return await self.outgoing.publish(
                Message(bytes(json.dumps(data), 'utf-8')), self.tag)
        elif type(data) in [float, int]:
            return await self.outgoing.publish(
                Message(bytes(str(data), 'utf-8')), self.tag)
        elif type(data) == str:
            return await self.outgoing.publish(
                Message(bytes(data, 'utf-8')), self.tag)
        else:
            raise Exception(f"Pushing data of type {type(data)} not supported.")
