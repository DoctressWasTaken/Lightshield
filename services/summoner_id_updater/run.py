import asyncio
import os
import logging
import json
import aiohttp
from datetime import datetime, timedelta
from aio_pika import Message
import aioredis
from worker_alternative import WorkerClass, ServiceClass

from exceptions import (
    RatelimitException,
    NotFoundException,
    Non200Exception,
    NoMessageException
)


class SummonerIDUpdater(ServiceClass):

    async def init(self):
        channel = await self.rabbitc.channel()
        # Incoming
        incoming = await channel.declare_queue(
            'SUMMONER_ID_IN_' + self.server, durable=True)
        # Outgoing
        outgoing = await channel.declare_exchange(
            f'SUMMONER_ID_OUT_{self.server}', type='direct',
            durable=True)
        # Output to the Match_History_Updater
        match_history_in = await channel.declare_queue(
            f'MATCH_HISTORY_IN_{self.server}',
            durable=True
        )
        await match_history_in.bind(outgoing, 'SUMMONER_V2')
        # Output to the DB
        db_in = await channel.declare_queue(
            f'DB_SUMMONER_IN_{self.server}',
            durable=True)

        await db_in.bind(outgoing, 'SUMMONER_V2')


class Worker(WorkerClass):

    async def init(self):
        await self.channel.set_qos(prefetch_count=5)
        self.incoming = await self.channel.declare_queue(
            'SUMMONER_ID_IN_' + self.service.server, durable=True)

        self.outgoing = await self.channel.declare_exchange(
            f'SUMMONER_ID_OUT_{self.service.server}', type='direct',
            durable=True)

    async def get_task(self):
        while not (msg := await self.incoming.get(no_ack=True, fail=False))\
                and not self.service.stopping:
            await asyncio.sleep(0.1)
        if not msg:
            return None
        content = json.loads(msg.body.decode('utf-8'))
        identifier = content['summonerId']
        if redis_entry := await self.service.redisc.hgetall(f"user:{identifier}"):
            package = {**content, **redis_entry}
            await self.outgoing.publish(
                Message(bytes(json.dumps({**content, **redis_entry}), 'utf-8')),
                routing_key="SUMMONER_V2"
            )
            return None
        if identifier in self.service.buffered_elements:
            return None
        self.service.buffered_elements[identifier] = True
        return [identifier, content, msg]

    async def process_task(self, session, data):
        identifier, content, msg = data
        url = self.service.url % identifier
        try:
            response = await self.fetch(session, url)
            await self.service.redisc.hmset_dict(
                f"user:{identifier}",
                {'puuid': response['puuid'],
                         'accountId': response['accountId']})
            
            await self.outgoing.publish(
                Message(
                    body=bytes(json.dumps({**json.loads(msg.body.decode('utf-8')), **response}), 'utf-8')),
                    routing_key="SUMMONER_V2")
        except (RatelimitException, NotFoundException, Non200Exception):
            return
        finally:
            del self.service.buffered_elements[identifier]

if __name__ == "__main__":
    service = SummonerIDUpdater(
        url_snippet="summoner/v4/summoners/%s",
        queues_out=["MATCH_HISTORY_IN_%s"])
    asyncio.run(service.run(Worker))
