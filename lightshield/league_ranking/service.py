import asyncio
import logging
from datetime import datetime

import aiohttp

from lightshield.league_ranking.rank_manager import RankManager


class Service:
    running = False
    empty_page = False
    next_page = 1
    daemon = None
    pages = None
    active_rank = None

    def __init__(self, name, config, handler):
        self.name = name
        self.logging = logging.getLogger("%s" % name)
        self.handler = handler
        self.rankmanager = RankManager(config, self.logging)
        self.retry_after = datetime.now()
        self.data = []
        self.url = (
                f"{handler.protocol}://{self.name.lower()}.api.riotgames.com/lol/"
                + "league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        )

    async def init(self):
        self.pages = asyncio.Queue()
        await self.rankmanager.init()

    async def worker(self, session):
        """Makes calls."""
        while not self.handler.is_shutdown:
            page = await self.pages.get()
            if self.empty_page and page <= self.empty_page:
                return
            url = self.url % (*self.active_rank, page)
            async with session.get(url, proxy=self.handler.proxy) as response:
                self.logging.debug(url)
                try:
                    data = await response.json()
                except aiohttp.ContentTypeError:
                    await self.pages.put(page)
                    continue
                if not response.status == 200:
                    if response.status == 430:
                        wait_until = datetime.fromtimestamp(data["Retry-At"])
                        seconds = (wait_until - datetime.now()).total_seconds()
                        seconds = max(0.1, seconds)
                        await asyncio.sleep(seconds)
                    elif response.status == 429:
                        await asyncio.sleep(0.5)
                    await self.pages.put(page)
                    continue
                if not data:
                    self.empty_page = max(self.empty_page or 0, page)
                    return
                self.data += data
                self.next_page += 1
                await self.pages.put(self.next_page)

    async def run(self):
        """Runner."""
        await self.init()
        while not self.handler.is_shutdown:
            self.active_rank = await self.rankmanager.get_next()
            workers = 5
            self.next_page = workers
            self.empty_page = False
            for i in range(1, workers + 1):
                await self.pages.put(i)
            self.data = []
            try:
                async with aiohttp.ClientSession() as session:
                    self.logging.debug("START %s %s.", *self.active_rank)
                    await asyncio.gather(
                        *[asyncio.create_task(self.worker(session)) for _ in range(workers)]
                    )
                self.logging.debug("DONE %s %s.", *self.active_rank)
            except asyncio.CancelledError:
                return
            self.logging.debug("INSERTING %s %s.", *self.active_rank)
            await self.update_data()
            self.logging.debug("INSERTED %s %s.", *self.active_rank)

            await self.rankmanager.update(key=self.active_rank)
        self.logging.info("< Exited service")

    async def update_data(self):
        """Update all changed users in the DB."""
        async with self.handler.postgres.acquire() as connection:
            try:
                latest = await connection.fetch(
                    """SELECT summoner_id,
                    rank,
                    division,
                    leaguepoints
                    FROM %s.ranking
                    WHERE rank = $1
                    AND division = $2
                    """
                    % self.name.lower(),
                    *self.active_rank,
                timeout=30,)
                preset = {}
                if latest:
                    for line in latest:
                        preset[line["summoner_id"]] = [
                            line["rank"],
                            line["division"],
                            line["leaguepoints"],
                        ]
                to_update = []
                already_added = []
                for new in self.data:
                    rank = [new["tier"], new["rank"], new["leaguePoints"]]
                    if (
                            new["summonerId"] not in preset
                            or preset[new["summonerId"]] != rank
                    ):
                        if new["summonerId"] not in already_added:
                            already_added.append(new["summonerId"])
                            to_update.append([new["summonerId"]] + rank)
                updated = len(to_update)
                while to_update:
                    batch = to_update[:5000]
                    await connection.executemany(
                        """INSERT INTO %s.ranking (summoner_id, rank, division, leaguepoints)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (summoner_id) DO 
                            UPDATE SET  rank = EXCLUDED.rank,
                                        division = EXCLUDED.division,
                                        leaguepoints = EXCLUDED.leaguepoints,
                                        last_updated = CURRENT_TIMESTAMP,
                                        defunct = FALSE 
                        """
                        % self.name.lower(),
                        batch,
                    timeout=30,)
                    if len(to_update) > 5000:
                        to_update = to_update[5000:]
                    else:
                        to_update = []
                self.logging.info(
                    "Updated %s users in %s %s.", updated, *self.active_rank
                )
            except Exception as err:
                self.logging.error(err)
