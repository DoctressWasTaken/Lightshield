"""League Updater Module."""
import asyncio
import logging
import os
import signal
from datetime import datetime, timedelta

import aiohttp
import asyncpg
import uvloop
from exceptions import RatelimitException, NotFoundException, Non200Exception
from rank_manager import RankManager

uvloop.install()

tiers = {
    "IRON": 0,
    "BRONZE": 1,
    "SILVER": 2,
    "GOLD": 3,
    "PLATINUM": 4,
    "DIAMOND": 5,
    "MASTER": 6,
    "GRANDMASTER": 6,
    "CHALLENGER": 6}

rank = {
    "IV": 0,
    "III": 1,
    "II": 2,
    "I": 3}


class Service:  # pylint: disable=R0902
    """Core service worker object."""
    empty = False
    stopped = False
    next_page = 1

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("LeagueRankings")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server.lower()}.api.riotgames.com/lol/" + \
                   "league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        self.rankmanager = RankManager()
        self.retry_after = datetime.now()

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def init(self):
        """Override of the default init function.

        Initiate the Rankmanager object.
        """
        await self.rankmanager.init()

    async def process_rank(self, tier, division, worker=5):

        task_sets = await asyncio.gather(
            *[asyncio.create_task(self.async_worker(tier, division, offset=i, worker=worker)) for i in range(worker)])
        tasks = {}
        for entry in task_sets:
            tasks = {**tasks, **entry}

        min_rank = tiers[tier] * 400 + rank[division] * 100
        self.logging.info("Found %s unique user.", len(tasks))

        conn = await asyncpg.connect("postgresql://na1@192.168.0.1/%s" % self.server.lower())
        latest = await conn.fetch('''
            SELECT summoner_id, rank, wins, losses
            FROM summoner
            WHERE rank >= $1 
            AND rank <= $2
        ''', min_rank, min_rank + 100)
        for line in latest:
            if line['summoner_id'] in tasks:
                task = tasks[line['summoner_id']]
                if task == (line['summoner_id'],
                            int(line['rank']),
                            int(line['wins']),
                            int(line['losses'])):
                    del tasks[line['summoner_id']]
        self.logging.info("Upserting %s changed user.", len(tasks))
        if tasks:
            await conn.execute('''
                INSERT INTO summoner (summoner_id, rank, wins, losses)
                    VALUES %s
                    ON CONFLICT (summoner_id) DO 
                    UPDATE SET rank = EXCLUDED.rank,
                               wins = EXCLUDED.wins,
                               losses = EXCLUDED.losses  
            ''' % ",".join(["('%s', %s, %s, %s)" % task for task in tasks.values()]))
        await conn.close()

    async def async_worker(self, tier, division, offset, worker):
        failed = False
        empty = False
        page = offset + 1
        tasks = []
        while (not empty or failed) and not self.stopped:
            if (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(delay)
            async with aiohttp.ClientSession() as session:
                try:
                    content = await self.fetch(session, url=self.url % (
                        tier, division, page))
                    if len(content) == 0:
                        self.logging.info("Page %s is empty.", page)
                        empty = True
                        continue
                    tasks += content
                except (RatelimitException, Non200Exception):
                    failed = True
                except NotFoundException:
                    empty = True
            if not failed:
                page += worker
            failed = False

        unique_tasks = {}
        for task in tasks:
            unique_tasks[task['summonerId']] = (
                task['summonerId'],
                int(tiers[task['tier']] * 400 + rank[task['rank']] * 100 + task['leaguePoints']),
                int(task['wins']),
                int(task['losses']))
        return unique_tasks

    async def fetch(self, session, url):
        """Execute call to external target using the proxy server.

        Receives aiohttp session as well as url to be called. Executes the request and returns
        either the content of the response as json or raises an exeption depending on response.
        :param session: The aiohttp ClientSession used to execute the call.
        :param url: String url ready to be requested.

        :returns: Request response as dict.

        :raises RatelimitException: on 429 or 430 HTTP Code.
        :raises NotFoundException: on 404 HTTP Code.
        :raises Non200Exception: on any other non 200 HTTP Code.
        """
        try:
            async with session.get(url, proxy="http://lightshield_proxy_%s:8000" % self.server.lower()) as response:
                await response.text()
                if response.status == 429:
                    self.logging.info(429)
        except aiohttp.ClientConnectionError as err:
            self.logging.info("Error %s", err)
            raise Non200Exception()
        if response.status in [429, 430]:
            if "Retry-After" in response.headers:
                delay = max(1, int(response.headers['Retry-After']))
                self.retry_after = datetime.now() + timedelta(seconds=delay)
            raise RatelimitException()
        if response.status == 404:
            raise NotFoundException()
        if response.status != 200:
            raise Non200Exception()
        return await response.json(content_type=None)

    async def run(self):
        """Override the default run method due to special case.

        Worker are started and stopped after each tier/rank combination.
        """
        await self.init()
        while not self.stopped:
            tier, division = await self.rankmanager.get_next()
            self.empty = False
            self.next_page = 1
            await self.process_rank(tier, division, worker=5)
            await self.rankmanager.update(key=(tier, division))


if __name__ == "__main__":
    service = Service()
    signal.signal(signal.SIGTERM, service.shutdown)
    asyncio.run(service.run())
