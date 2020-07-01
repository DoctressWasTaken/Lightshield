"""Match History updater. Pulls matchlists for all player."""

import asyncio
import json

import uvloop
import signal
import os
from aio_pika import Message, DeliveryMode
import aioredis
from worker_alternative import WorkerClass, ServiceClass

from exceptions import (
    RatelimitException,
    NotFoundException,
    Non200Exception,
    NoMessageException
)



class MatchUpdater(ServiceClass):

    async def init(self):

        channel = await self.rabbitc.channel()
        # Incoming
        incoming = await channel.declare_queue(
            'MATCH_IN_' + self.server, durable=True)
        # Outgoing
        outgoing = await channel.declare_exchange(
            f'MATCH_OUT_{self.server}', type='direct',
            durable=True)
        db_in = await channel.declare_queue(
            'DB_MATCH_IN_' + self.server, durable=True)
        await db_in.bind(outgoing, 'MATCH')

        await self.pika.init(incoming=incoming, outgoing=outgoing, tag='MATCH')


class Worker(WorkerClass):

    async def init(self):
        await self.channel.set_qos(prefetch_count=5)
        self.incoming = await self.service.channel.declare_queue(
            'MATCH_IN_' + self.server, durable=True)

        self.outgoing = await self.service.channel.declare_exchange(
            f'MATCH_OUT_{self.server}', type='direct',
            durable=True)

    async def get_task(self):
        while not (msg := await self.incoming.get(no_ack=False, fail=False)):
            await asyncio.sleep(0.1)

        identifier = msg.body.decode('utf-8')

        if prev := await self.redis.sismember('matches', str(identifier)):
            await msg.ack()
            return None

        if identifier in self.service.buffered_elements:
            await msg.ack()
            return None

        self.service.buffered_elements[identifier] = True
        return [identifier, msg]

    async def process_task(self, session, data):
        identifier, msg = data
        url = self.service.url % identifier

        try:
            response = await self.fetch(session, url)
            await self.service.redisc.sadd('matches', identifier)

            await self.outgoing.publish(
                Message(body=bytes(data, 'utf-8'),
                        delivery_mode=DeliveryMode.PERSISTENT),
                routing_key="MATCH")

        except (RatelimitException, Non200Exception):
            await msg.reject(requeue=True)
        except NotFoundException:
            await msg.reject(requeue=False)
        finally:
            del self.service.buffered_elements[identifier]


if __name__ == "__main__":
    service = MatchUpdater(
        url_snippet="match/v4/matches/%s",
        queues_out=[])
    asyncio.run(service.run(Worker))