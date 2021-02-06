"""Match History updater. Pulls matchlists for all player."""
import asyncio
import logging
import os
import traceback
from datetime import datetime

import aiohttp
import aioredis
import asyncpg
from exceptions import NotFoundException


class Service:
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("MatchHistory")
        level = logging.INFO
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + \
                   "match/v4/matchlists/by-account/%s?beginIndex=%s&endIndex=%s"
        self.stopped = False
        self.retry_after = datetime.now()

        self.buffered_elements = {}  # Short term buffer to keep track of currently ongoing requests

        self.active_tasks = []

    async def init(self):
        """Initiate timelimit for pulled matches."""
        self.redis = await aioredis.create_redis_pool(
            ('redis', 6379), encoding='utf-8')

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def flush_manager(self, matches, account_id, keys):
        """Update entries in postgres once enough tasks are done."""
        try:
            sets = []
            for entry in matches:
                sets.append("(%s, %s, TO_TIMESTAMP(%s)::timestamp)" % (
                    entry['gameId'], entry['queue'], entry['timestamp'] // 1000
                ))
            conn = await asyncpg.connect("postgresql://postgres@postgres/raw")
            await conn.execute('''
                INSERT INTO match (match_id, queue, timestamp)
                VALUES %s
                ON CONFLICT DO NOTHING;
                ''' % ",".join(sets))
            await conn.execute('''
                UPDATE summoner
                SET wins_last_updated = $1,
                    losses_last_updated = $2
                WHERE account_id = $3
                ''', int(keys['wins']), int(keys['losses']), account_id)
            await conn.close()
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

    async def get_task(self):
        """Return tasks to the async worker."""
        while not (task := await self.redis.zpopmax('match_history_tasks', 1)) and not self.stopped:
            await asyncio.sleep(5)
            self.logging.info(task)
        if self.stopped:
            return
        keys = await self.redis.hgetall('%s:%s' % (task[0], task[1]))
        await self.redis.delete('%s:%s' % (task[0], task[1]))
        start = int(datetime.utcnow().timestamp())
        await self.redis.zadd('summoner_id_in_progress', start, task[0])
        return [task[0], int(task[1])], keys

    async def full_refresh(self, account_id, keys):
        """Pull match-history data until the page is empty."""
        empty = False
        worker = 3
        async with aiohttp.ClientSession() as session:
            match_data = await asyncio.gather(*[
                asyncio.create_task(self.worker(
                    account_id, session=session, offset=i, worker=worker
                )) for i in range(worker)
            ])
        if self.stopped:
            return
        matches = []
        for data in match_data:
            matches += data
        await self.flush_manager(matches, account_id, keys)

    async def partial_refresh(self, account_id, to_call, keys):
        """Pull match-history data corresponding to how much"""
        empty = False
        worker = 3
        pages = to_call // 100 + 1
        async with aiohttp.ClientSession() as session:
            match_data = await asyncio.gather(*[
                asyncio.create_task(self.worker(
                    account_id, session=session, offset=i, worker=worker, limit=pages
                )) for i in range(worker)
            ])
        if self.stopped:
            return
        matches = []
        for data in match_data:
            matches += data
        await self.flush_manager(matches, account_id, keys)

    async def worker(self, account_id, session, offset, worker, limit=None) -> list:
        """Multiple started per separate processor.
        Does calls continuously until it reaches an empty page."""
        matches = []
        start = offset * 100
        end = offset * 100 + 100
        while not self.stopped:
            if limit and start > limit * 100:
                return [match for match in matches if match['platformId'] == self.server.upper()]
            try:
                result = await self.fetch(session=session,
                                          url=self.url % (account_id, start, end))
                if not result['matches']:
                    return [match for match in matches if match['platformId'] == self.server.upper()]
                matches += result['matches']
                start += 100 * worker
                end += 100 * worker
                await asyncio.sleep(0.2)
            except NotFoundException:
                return [match for match in matches if match['platformId'] == self.server.upper()]
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                self.logging.info(err)
        return []

    async def async_worker(self):
        while not self.stopped:
            if not (data := await self.get_task()):
                continue
            task, keys = data
            if task[1] == 9999:
                await self.full_refresh(task[0], keys)
            else:
                await self.partial_refresh(*task, keys)

    async def fetch(self, session, url) -> dict:
        """
        Execute call to external target using the proxy server.

        Receives aiohttp session as well as url to be called.
        Executes the request and returns either the content of the
        response as json or raises an exeption depending on response.
        :param session: The aiohttp Clientsession used to execute the call.
        :param url: String url ready to be requested.

        :returns: Request response as dict.
        :raises RatelimitException: on 429 or 430 HTTP Code.
        :raises NotFoundException: on 404 HTTP Code.
        :raises Non200Exception: on any other non 200 HTTP Code.
        """

        try:
            async with session.get(url, proxy="http://lightshield_proxy_%s:8000" % self.server.lower()) as response:
                await response.text()
        except aiohttp.ClientConnectionError:
            raise Non200Exception()
        if response.status in [429, 430]:
            if "Retry-After" in response.headers:
                delay = int(response.headers['Retry-After'])
                self.retry_after = datetime.now() + timedelta(seconds=delay)
            raise RatelimitException()
        if response.status == 404:
            raise NotFoundException()
        if response.status != 200:
            raise Non200Exception()
        return await response.json(content_type=None)

    async def run(self):
        """
        Runner.
        """
        await self.init()
        self.logging.info("Initiated.")
        await asyncio.gather(*[
            asyncio.create_task(self.async_worker()) for _ in range(5)
        ])
