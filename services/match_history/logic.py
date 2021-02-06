"""Match History updater. Pulls matchlists for all player."""
import asyncio
import logging
import os
import traceback
from datetime import datetime, timedelta

import aiohttp
import aioredis
import asyncpg
from exceptions import RatelimitException, NotFoundException, Non200Exception


class Service:
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("MatchHistory")
        level = logging.INFO
        if "LOGGING" in os.environ:
            level = getattr(logging, os.environ['LOGGING'])
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + \
                   "match/v4/matchlists/by-account/%s?beginIndex=%s&endIndex=%s&queue=420"
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

    async def flush_manager(self):
        """Update entries in postgres once enough tasks are done."""
        try:
            conn = await asyncpg.connect("postgresql://postgres@postgres/raw")
            if self.completed_tasks:
                self.logging.info("Inserting %s summoner IDs.", len(self.completed_tasks))
                await conn.executemany('''
                    UPDATE summoner
                    SET account_id = $1, puuid = $2
                    WHERE summoner_id = $3;
                    ''', self.completed_tasks)
                self.completed_tasks = []
            if self.to_delete:
                await conn.execute('''
                    DROP FROM summoner
                    WHERE summoner_id IN (%s);
                    ''', ",".join(["'%s'" % id for id in self.to_delete]))
                self.to_delete = []

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
        start = int(datetime.utcnow().timestamp())
        await self.redis.zadd('summoner_id_in_progress', start, task)
        return task

    """
    async def async_worker(self):
        while not self.stopped:
            try:
                async with aiohttp.ClientSession() as session:

                        id = ids.pop()
                        calls_in_progress.append(asyncio.create_task(
                            self.handler(
                                session=session,
                                url=self.url % (account_id, id, id + 100)
                            )
                        ))
                        await asyncio.sleep(0.1)
                        responses = await asyncio.gather(*calls_in_progress)
                        match_data = list(set().union(*responses))
                        query = 'REPLACE INTO match_history (accountId, matches) VALUES (\'%s\', %s);' % (
                            account_id, matches)
                        await self.marker.execute_write(query)
                        conn = await asyncpg.connect("postgresql://postgres@postgres/raw")

                        result = await conn.execute('''
                            INSERT INTO match ("matchId")
                            VALUES %s
                            ON CONFLICT ("matchId")
                            DO NOTHING;
                        ''' % ",".join(["(%s)" % matchId for matchId in match_data]))
                        await conn.close()
                        self.logging.debug(result)
            except NotFoundException:
                return
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                self.logging.info(err)
            finally:
                self.logging.debug("Finished task.")
                del self.buffered_elements[account_id]
    """
    async def fetch(self, session, url):
        """Execute call to external target using the proxy server.

        Receives aiohttp session as well as url to be called. Executes the request and returns
        either the content of the response as json or raises an exeption depending on response.
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

    async def handler(self, session, url):
        rate_flag = False
        while not self.stopped:
            if datetime.now() < self.retry_after or rate_flag:
                rate_flag = False
                delay = max(0.5, (self.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                response = await self.fetch(session, url)
                return [match['gameId'] for match in response['matches'] if
                        match['queue'] == 420 and
                        match['platformId'] == self.server and
                        int(str(match['timestamp'])[:10]) >= self.timelimit]

            except RatelimitException:
                rate_flag = True
            except Non200Exception:
                await asyncio.sleep(0.1)

    async def run(self):
        """Runner."""
        await self.init()
        self.logging.info("Initiated.")
        while not self.stopped:
            await asyncio.sleep(1)
