"""League Updater Module."""
import aiohttp
import asyncio
import asyncpg
import logging
import os
import signal
import uvloop
from datetime import datetime, timedelta

if "DEBUG" in os.environ:
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
else:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
from lightshield import settings
from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
)
from lightshield.proxy import Proxy

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
    "CHALLENGER": 6,
}

rank = {"IV": 0, "III": 1, "II": 2, "I": 3}


class Service:  # pylint: disable=R0902
    """Core service worker object."""

    empty = False
    stopped = False
    next_page = 1

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("Service")

        # Postgres
        self.db = None
        # Proxy
        self.proxy = Proxy()
        self.endpoint_url = (
            f"https://{settings.SERVER}.api.riotgames.com/lol/league-exp/v4/entries/"
        )
        self.endpoint = None
        self.url = (
            f"https://{settings.SERVER}.api.riotgames.com/lol/"
            + "league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        )
        self.rankmanager = RankManager()
        self.retry_after = datetime.now()

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def init(self):
        await self.rankmanager.init()
        self.db = await asyncpg.create_pool(
            host=settings.PERSISTENT_HOST,
            port=settings.PERSISTENT_PORT,
            user='postgres',
            password=settings.PERSISTENT_PASSWORD,
            database=settings.PERSISTENT_DATABASE,
        )

        await self.proxy.init(settings.PROXY_SYNC_HOST, settings.PROXY_SYNC_PORT)
        self.logging.info(self.endpoint_url)
        self.endpoint = await self.proxy.get_endpoint(self.endpoint_url)

    async def process_rank(self, tier, division, worker=5):

        task_sets = await asyncio.gather(
            *[
                asyncio.create_task(
                    self.async_worker(tier, division, offset=i, worker=worker)
                )
                for i in range(worker)
            ]
        )
        tasks = {}
        for entry in task_sets:
            tasks = {**tasks, **entry}

        min_rank = tiers[tier] * 400 + rank[division] * 100
        self.logging.info("Found %s unique user.", len(tasks))

        async with self.db.acquire() as connection:
            latest = await connection.fetch(
                """
                SELECT summoner_id, rank, wins, losses
                FROM %s.summoner
                WHERE rank >= $1 
                AND rank <= $2
            """
                % settings.SERVER,
                min_rank,
                min_rank + 100,
            )
            for line in latest:
                if line["summoner_id"] in tasks:
                    task = tasks[line["summoner_id"]]
                    if task == (
                        line["summoner_id"],
                        int(line["rank"]),
                        int(line["wins"]),
                        int(line["losses"]),
                    ):
                        del tasks[line["summoner_id"]]
            self.logging.info("Upserting %s changed user.", len(tasks))
            if tasks:
                try:
                    await connection.executemany(
                        """INSERT INTO %s.summoner (summoner_id, rank, wins, losses)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (summoner_id) DO 
                            UPDATE SET rank = EXCLUDED.rank,
                                       wins = EXCLUDED.wins,
                                       losses = EXCLUDED.losses  
                        """
                        % settings.SERVER,
                        tasks.values(),
                    )
                except Exception as err:
                    self.logging.critical(err)

    async def async_worker(self, tier, division, offset, worker):
        failed = False
        empty = False
        page = offset + 1
        tasks = []
        while (not empty or failed) and not self.stopped:
            while (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(min(0.1, delay))
            async with aiohttp.ClientSession(
                headers={"X-Riot-Token": settings.API_KEY}
            ) as session:
                try:
                    content = await self.fetch(
                        session, url=self.url % (tier, division, page)
                    )
                    if len(content) == 0:
                        self.logging.info("Page %s is empty.", page)
                        empty = True
                        continue
                    tasks += content
                except LimitBlocked as err:
                    self.retry_after = datetime.now() + timedelta(
                        seconds=err.retry_after
                    )
                except (RatelimitException, Non200Exception):
                    failed = True
                except NotFoundException:
                    empty = True
            if not failed:
                page += worker
            failed = False

        unique_tasks = {}
        for task in tasks:
            unique_tasks[task["summonerId"]] = (
                task["summonerId"],
                int(
                    tiers[task["tier"]] * 400
                    + rank[task["rank"]] * 100
                    + task["leaguePoints"]
                ),
                int(task["wins"]),
                int(task["losses"]),
            )
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
        return await self.endpoint.request(url, session)

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
        await self.db.close()


if __name__ == "__main__":
    service = Service()
    signal.signal(signal.SIGTERM, service.shutdown)
    asyncio.run(service.run())
