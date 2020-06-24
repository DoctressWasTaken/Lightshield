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

    async def init(self):
        """Initiate channel and exchanges."""
        await self.connect()

        channel = await self.rabbit.channel()
        await channel.set_qos(prefetch_count=1)
        # Incoming
        self.rabbit_queue = await channel.declare_queue(
            'SUMMONER_ID_IN_' + self.server, durable=True)
        # Outgoing
        self.rabbit_exchange = await channel.declare_exchange(
            f'SUMMONER_ID_OUT_{self.server}', type='direct',
            durable=True)

        # Output to the Match_History_Updater
        match_history_in = await channel.declare_queue(
            f'MATCH_HISTORY_IN_{self.server}',
            durable=True
        )
        await match_history_in.bind(self.rabbit_exchange, 'SUMMONER_V2')

        # Output to the DB
        db_in = await channel.declare_queue(
            f'DB_SUMMONER_IN_{self.server}',
            durable=True)

        await db_in.bind(self.rabbit_exchange, 'SUMMONER_V2')

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

    async def get(self):
        """Get message from rabbitmq.

        Returns either the message element or None on timeout.
        """
        try:
            return await self.rabbit_queue.get(timeout=0.5)
        except Exception as err:
            print(err)
            return None

    async def push(self, data):
        """Push data through exchange.

        The data is used by both db_worker and match_history_updater.
        """
        return await self.rabbit_exchange.publish(
            Message(bytes(json.dumps(data), 'utf-8')), 'SUMMONER_V2')
