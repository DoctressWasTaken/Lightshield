"""Match History updater. Pulls matchlists for all player."""

import asyncio
import json
import os
from datetime import datetime
from aio_pika import Message
import aioredis
from worker_alternative import WorkerClass, ServiceClass

from exceptions import (
    RatelimitException,
    NotFoundException,
    Non200Exception,
    NoMessageException
)


server = os.environ['SERVER']


class MatchHistoryUpdater(ServiceClass):

    async def init(self):
        self.required_matches = int(os.environ['MATCHES_TO_UPDATE'])
        try:
            self.timelimit = int(os.environ['TIME_LIMIT'])
        except:
            self.timelimit = None
        channel = await self.rabbitc.channel()
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


class Worker(WorkerClass):

    async def init(self):
        await self.channel.set_qos(prefetch_count=5)
        self.incoming = await self.channel.declare_queue(
            'MATCH_HISTORY_IN_' + self.service.server, durable=True)
        # Outgoing
        self.outgoing = await self.channel.declare_exchange(
            f'MATCH_HISTORY_OUT_{self.service.server}', type='direct',
            durable=True)

    async def get_task(self):
        while not (msg := await self.incoming.get(no_ack=True, fail=False)):
            await asyncio.sleep(0.1)

        content = json.loads(msg.body.decode('utf-8'))
        identifier = content['summonerId']

        matches = content['wins'] + content['losses']
        if prev := await self.service.redisc.hgetall(f"user:{identifier}"):
            matches -= (int(prev['wins']) + int(prev['losses']))
        if matches < self.required_matches:  # Skip if less than required new matches
            # TODO: Despite not having enough matches this should be considered to pass on to the db
            return None

        if identifier in self.buffered_elements:
            return None

        self.service.buffered_elements[identifier] = True

        return [identifier, content, msg, matches]

    async def process_task(self, session, data):
        identifier, content, msg, matches = data

        matches_to_call = matches + 3
        calls = int(matches_to_call / 100) + 1
        ids = [start_id * 100 for start_id in range(calls)]
        calls_in_progress = []
        matches = []
        while ids:
            id = ids.pop()
            calls_in_progress.append(asyncio.create_task(
                self.handler(
                    session=session,
                    url=self.service.url % (content['accountId'], id, id + 100)
                )
            ))
            await asyncio.sleep(0.1)
        try:
            responses = await asyncio.gather(*calls_in_progress)
            matches = list(set().union(*responses))
            await self.service.redisc.hmset_dict(
                f"user:{identifier}",
                {
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
                await self.outgoing.publish(
                    Message(bytes(id, 'utf-8')),
                    routing_key="MATCH"
                )
        except NotFoundException:
            pass
        finally:
            del self.service.buffered_elements[identifier]

    async def handler(self, session, url):
        rate_flag = False
        while True:
            if datetime.now() < self.service.retry_after or rate_flag:
                rate_flag = False
                delay = max(0.5, (self.service.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                response = await self.fetch(session, url)
                if not self.service.timelimit:
                    return [match['gameId'] for match in response['matches'] if
                            match['queue'] == 420 and
                            match['platformId'] == server]
                return [match['gameId'] for match in response['matches'] if
                        match['queue'] == 420 and
                        match['platformId'] == server and
                        int(str(match['timestamp'])[:10]) >= self.service.timelimit]

            except RatelimitException:
                rate_flag = True
            except Non200Exception:
                pass


if __name__ == "__main__":
    service = MatchHistoryUpdater(
        url_snippet="match/v4/matchlists/by-account/%s?beginIndex=%s&endIndex=%s&queue=420",
        queues_out=["MATCH_IN_%s"])
    asyncio.run(service.run(Worker))
