import asyncio
import os
import logging
import json
import aiohttp
from datetime import datetime, timedelta

from worker import Worker, RatelimitException, NotFoundException, Non200Exception


class SummonerIDUpdater(Worker):

    async def initiate_pika(self, connection):
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=100)
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

        await self.pika.init(incoming=incoming, outgoing=outgoing, tag='SUMMONER_V2', no_ack=True)

    async def is_valid(self, identifier, content, msg):

        if redis_entry := await self.redis.hgetall(f"user:{identifier}"):
            package = {**content, **redis_entry}
            await self.pika.push(package)
            return False
        return {"foo": "bar"}

    async def process(self, session, identifier, msg, **kwargs):
        url = self.url_template % (identifier)
        self.logging.debug(f"Fetching {url}")
        try:
            response = await self.fetch(session, url)
            return [0, identifier, response, msg]
        except RatelimitException:
            return [1]
        except NotFoundException:
            return [1]
        except Non200Exception:
            return [1]
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
