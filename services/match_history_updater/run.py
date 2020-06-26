"""Match History updater. Pulls matchlists for all player."""

import asyncio
import json
import os
import logging
import aiohttp
from datetime import datetime, timedelta

from worker import (
    Worker,
    RatelimitException,
    NotFoundException,
    Non200Exception,
    NoMessageException
)

from redis_connector import Redis
from pika_connector import Pika

if "SERVER" not in os.environ:
    print("No SERVER env variable provided. exiting")
    exit()

server = os.environ['SERVER']


class MatchHistoryUpdater(Worker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.required_matches = int(os.environ['MATCHES_TO_UPDATE'])


    async def initiate_pika(self, connection):

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        # Incoming
        incoming = await channel.declare_queue(
            'MATCH_HISTORY_IN_' + self.server, durable=True)
        # Outgoing
        outgoing = await channel.declare_exchange(
            f'MATCH_HISTORY_OUT_{self.server}', type='direct',
            durable=True)

        # Output to the Match_Updater
        match_in = await channel.declare_queue(
            f'MATCH_IN_{self.server}',
            durable=True
        )
        await match_in.bind(outgoing, 'MATCH')

        await self.pika.init(incoming=incoming, outgoing=outgoing, tag='MATCH')

    async def is_valid(self, identifier, content, msg):
        """Return true if the msg should be passed on.

        If not valid this method properly handles the msg.
        """
        matches = content['wins'] + content['losses']

        if prev := await self.redis.hgetall(f"user:{identifier}"):
            matches -= (int(prev['wins']) + int(prev['losses']))

        if matches < self.required_matches:  # Skip if less than required new matches
            await self.pika.ack(msg)
            # TODO: Despite not having enough matches this should be considered to pass on to the db
            return False
        return {"matches": matches}

    async def handler(self, session, url):
        while True:
            if datetime.now() < self.retry_after:
                delay = (self.retry_after - datetime.now()).total_seconds()
                await asyncio.sleep(delay)
            try:
                response = await self.fetch(session, url)
                return [match['gameId'] for match in response['matches'] if
                    match['queue'] == 420 and match['platformId'] == server]
            except RatelimitException:
                pass
            except Non200Exception:
                pass

    async def worker(self, session, identifier, msg, matches):
        """Manage a single summoners full history calls."""
        matches_to_call = matches + 3
        calls = int(matches_to_call / 100) + 1
        ids = [start_id * 100 for start_id in range(calls)]
        content = json.loads(msg.body.decode('utf-8'))
        calls_executed = []
        while ids:
            id = ids.pop()
            calls_executed.append(asyncio.create_task(
                self.handler(
                    session=session,
                    url=self.url_template % (content['accountId'], id, id + 100))
            ))
            await asyncio.sleep(0.01)
        try:
            responses = await asyncio.gather(*calls_executed)
            matches = list(set().union(*responses))
            await self.redis.hset(
                key=f"user:{identifier}",
                mapping={
                    "summonerName": content['summonerName'],
                    "wins": content['wins'],
                    "losses": content['losses'],
                    "tier": content['tier'],
                    "rank": content['rank'],
                    "leaguePoints": content['leaguePoints']
                }
            )
            while matches:
                id = matches.pop()
                await self.pika.push(id)
                await self.pika.ack(msg)
        except NotFoundException:  # Triggers only if a call returns 404. Forces a full reject.
            await self.pika.reject(msg, requeue=False)
        finally:
            del self.buffered_summoners[identifier]


if __name__ == "__main__":
    buffer = int(os.environ['BUFFER'])
    worker = MatchHistoryUpdater(
        buffer=buffer,
        url=f"http://{os.environ['SERVER']}.api.riotgames.com/lol/match/v4/matchlists/by-account/" \
           "%s?beginIndex=%s&endIndex=%s&queue=420",
        identifier="summonerId")
    asyncio.run(worker.main())
