"""Match History updater. Pulls matchlists for all player."""

import asyncio
import json
import os
import time
import pika
import redis
import websockets
from datetime import datetime, timedelta

if "SERVER" not in os.environ:
    print("No SERVER env variable provided. exiting")
    exit()

server = os.environ['SERVER']

import logging
import aiohttp
from redis_connector import Redis
from pika_connector import Pika


class Worker:

    def __init__(self, buffer, required_matches):

        self.redis = Redis()
        self.pika = Pika()
        self.url_template =f"http://{server}.api.riotgames.com/lol/match/v4/matchlists/by-account/" \
           "%s?beginIndex=%s&endIndex=%s&queue=420"

        self.max_buffer = buffer
        self.failed_tasks = []
        self.retry_after = None
        self.required_matches = required_matches
        self.buffered_summoners = {}

        self.logging = logging.getLogger("worker")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [WORKER] %(message)s'))
        self.logging.addHandler(ch)


    async def fetch(self, session, url, startingId):
        self.logging.debug(f"Fetching {url}.")
        async with session.get(url, proxy="http://proxy:8000") as response:
            try:
                resp = await response.json(content_type=None)
            except:
                pass
            if response.status in [429, 430]:
                if "Retry-After" in response.headers:
                    delay = int(response.headers['Retry-After'])
                    self.retry_after = datetime.now() + timedelta(
                        seconds=delay)

            if response.status != 200:
                return {"status": "failed", "id": startingId}
            else:
                return {"status": "success",
                        "id": startingId,
                        "matches": [match['gameId'] for match in resp['matches'] if match['queue'] == 420 and match['platformId'] == server]}


    async def process_history(self, msg, summonerId, matches):
        """Manage a single summoners full history calls."""
        matches_to_call = matches + 3
        calls = int(matches_to_call / 100) + 1
        ids = [start_id * 100 for start_id in range(calls)]
        content = json.loads(msg.body.decode('utf-8'))
        matches = []
        while ids:
            async with aiohttp.ClientSession() as session:
                calls_executed = []
                while ids:
                    if self.retry_after and datetime.now() < self.retry_after:
                        delay = (self.retry_after - datetime.now()).total_seconds()
                        await asyncio.sleep(delay)
                    id = ids.pop()
                    calls_executed.append(asyncio.create_task(
                        self.fetch(session=session,
                                   url=self.url_template % (content['accountId'], id, id + 100),
                                   startingId=id)
                    ))
                    await asyncio.sleep(0.1)
                responses = await asyncio.gather(*calls_executed)
            for response in responses:
                if response['status'] == "failed":
                    ids.append(response['id'])
                else:
                    matches += response['matches']

        matches = list(set(matches))
        await self.redis.hset(summonerId,
             mapping={
                 "summonerName": content['summonerName'],
                 "wins": content['wins'],
                 "losses": content['losses'],
                 "tier": content['tier'],
                 "rank": content['rank'],
                 "leaguePoints": content['leaguePoints']
             })
        for match_id in matches:
            await self.pika.push(match_id)
        del self.buffered_summoners[summonerId]
        msg.ack()

    async def next_task(self):
        while True:
            msg = await self.pika.get()
            if not msg:
                self.logging.info("No messages found. Awaiting.")
                while not msg:
                    msg = await self.pika.get()
                    await asyncio.sleep(1)
                self.logging.info("Continuing.")
            content = json.loads(msg.body.decode('utf-8'))
            summonerId = content['summonerId']

            if summonerId in self.buffered_summoners:  # Skip any further tasks for already queued
                self.logging.info(f"Summoner {summonerId} is already registered as an active task.")
                try:
                    await msg.ack()
                except:
                    self.logging.info(f"Failed to ack {summonerId}.")
                continue

            prev = await self.redis.hgetall(summonerId)
            matches = content['wins'] + content['losses']

            if prev:
                matches -= (int(prev['wins']) + int(prev['losses']))

            if matches < self.required_matches:  # Skip if less than required new matches
                msg.ack()
                continue
            self.buffered_summoners[summonerId] = True
            return summonerId, matches, msg

    async def main(self):
        tasks = []
        while True:
            if len(self.buffered_summoners) >= self.max_buffer:  # Only Queue when below buffer limit
                self.logging.info("Buffer full. Waiting.")
                while len(self.buffered_summoners) >= self.max_buffer:
                    await asyncio.sleep(0.5)
                self.logging.info("Continue")

            summonerId, matches, msg = await self.next_task()
            tasks.append(asyncio.create_task(self.process_history(
                msg=msg, summonerId=summonerId, matches=matches)))
            await asyncio.sleep(0.03)
        await asyncio.gather(*tasks)

    async def run(self):
        await self.pika.init()
        await self.main()

if __name__ == "__main__":
    buffer = int(os.environ['MATCH_HISTORY_BUFFER'])
    required_matches = int(os.environ['MATCHES_TO_UPDATE'])
    worker = Worker(buffer=buffer, required_matches=required_matches)
    asyncio.run(worker.run())
