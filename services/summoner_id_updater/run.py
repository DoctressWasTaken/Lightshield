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
            'SUMMONER_ID_IN_' + self.server, durable=True)

        self.outgoing = await self.channel.declare_exchange(
            f'SUMMONER_ID_OUT_{self.server}', type='direct',
            durable=True)

        self.session = aiohttp.ClientSession()

    async def get_task(self):
        while not (msg := self.incoming.get(no_ack=True, fail=False)):
            await asyncio.sleep(0.1)

        content = json.loads(msg.body.decode('utf-8'))
        identifier = content['summonerId']

        if redis_entry := await self.redis.hgetall(f"user:{identifier}"):
            package = {**content, **redis_entry}
            await self.outgoing.publish(
                Message(bytes(json.dumps({**content, **redis_entry}), 'utf-8')),
                f'MATCH_HISTORY_IN_{self.service.server}'
            )
            return None
        if identifier in self.service.buffered_elements:
            return None
        self.service.buffered_elements[identifier] = True

        return [identifier, content, msg]

    async def process_task(self, data):
        identifier, content, msg = data
        url = self.service.url % identifier
        try:
            response = await self.fetch(self.session, url)
            await self.redis.hmset_dict(
                f"user:{identifier}",
                {'puuid': response['puuid'],
                         'accountId': response['accountId']})
            await self.outgoing.publish(
                Message(bytes(json.dumps({**json.loads(msg.body.decode('utf-8')), **response}), 'utf-8')),
                f'MATCH_HISTORY_IN_{self.service.server}'
            )
        except (RatelimitException, NotFoundException, Non200Exception):
            pass
        finally:
            del self.buffered_elements[identifier]

    async def finalize(self, responses):
        for identifier, response, msg in [entry[1:] for entry in responses if entry[0] == 0]:
            await self.redis.hset(
                key=f"user:{identifier}",
                mapping={'puuid': response['puuid'],
                         'accountId': response['accountId']})
            await self.pika.push({**json.loads(msg.body.decode('utf-8')), **response})


if __name__ == "__main__":
    buffer = int(os.environ['BUFFER'])
    worker = SummonerIDUpdater(
        buffer=buffer,
        url=f"http://{os.environ['SERVER']}.api.riotgames.com/lol/summoner/v4/summoners/%s",
        identifier="summonerId",
        message_out=f"MATCH_HISTORY_IN_{os.environ['SERVER']}")
    asyncio.run(worker.main())
