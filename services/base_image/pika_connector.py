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

    async def init(self, incoming, outgoing, tag, no_ack=False):
        """Initiate in and outputs."""
        self.incoming = incoming
        self.outgoing = outgoing
        self.tag = tag
        self.no_ack = no_ack

    async def connect(self):
        """Connect to rabbitmq.

        :raises:: ConnectionError if connection can't be established.
        """
        if self.rabbit:
            await self.rabbit.close()
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
                return await asyncio.wait_for(msg.ack(), timeout=2)
            except asyncio.TimeoutError:
                self.logging.info(f"[Ack] Timeout error.")
                await asyncio.sleep(0.5)
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
                await asyncio.wait_for(msg.reject(requeue=requeue), timeout=10)
                return
            except asyncio.TimeoutError:
                self.logging.info(f"[Reject:{requeue}] Timeout error.")
                await asyncio.sleep(0.5)
            except Exception as err:
                self.logging.info(f"[Reject:{requeue}] Got exception {err.__class__.__name__}: {repr(err)}")
                return
        raise Exception("Failed to requeue message.")

    async def get(self):
        """Get message from rabbitmq.

        Returns either the message element or None on timeout.
        """
        for i in range(5):
            try:
                msg = await asyncio.wait_for(self.incoming.get(no_ack=self.no_ack, fail=False), timeout=10)
                return msg
            except asyncio.TimeoutError:
                self.logging.info(f"[Get] Timeout error.")
                await asyncio.sleep(0.5)
            except Exception as err:
                self.logging.info(f"[Get] Got exception {err.__class__.__name__}: {repr(err)}")
                return None
        raise Exception("Failed to get message.")

    async def push(self, data, persistent=False):
        """Push data through exchange.

        The data is used by both db_worker and match_history_updater.
        """
        if type(data) in [dict, list]:
           data = json.dumps(data)
        elif type(data) in [float, int]:
            data = str(data)
        else:
            raise Exception(f"Pushing data of type {type(data)} not supported.")

        if persistent:
            message = Message(
                    body=bytes(data, 'utf-8'),
                    delivery_mode=DeliveryMode.PERSISTENT)
        else:
            message = Message(bytes(data, 'utf-8'))

        for i in range(3):
            try:
                return await asyncio.wait_for(self.outgoing.publish(message, self.tag), timeout=10)
            except asyncio.TimeoutError:
                self.logging.info(f"[Send:{persistent}] Timeout error.")
                await asyncio.sleep(0.5)
            except Exception as err:
                self.logging.info(f"[Send:{persistent}] Got exception {err.__class__.__name__}: {repr(err)}")
                return
        raise Exception("Failed to send message.")

