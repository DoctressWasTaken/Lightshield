import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp

from lightshield.exceptions import (
    LimitBlocked,
    RatelimitException,
    NotFoundException,
    Non200Exception,
)
from rank_manager import RankManager


class Service:
    running = False
    empty_page = False
    next_page = 1
    daemon = None
    pages = None
    active_rank = None

    def __init__(self, name, handler):
        self.name = name
        self.logging = logging.getLogger("%s" % name)
        self.handler = handler
        self.rankmanager = RankManager(self.logging)
        self.retry_after = datetime.now()
        self.data = []
        self.url = (
                f"https://{self.name}.api.riotgames.com/lol/"
                + "league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        )

    async def init(self):
        self.pages = asyncio.Queue()
        await self.rankmanager.init()
        self.endpoint = await self.handler.proxy.get_endpoint(
            server=self.name, zone="league-v4-exp"
        )
        self.logging.info("Ready.")
        self.daemon = asyncio.create_task(self.runner())

    async def start(self):
        """Start the service calls."""
        if not self.running:
            self.logging.info("Started service calls.")
        self.running = True

    async def stop(self):
        """Halt the service calls."""
        if self.running:
            self.logging.info("Stopped service calls.")
        self.running = False

    async def worker(self):
        """Makes calls."""
        while not self.handler.is_shutdown:
            if not self.running:
                await asyncio.sleep(5)
                continue
            page = await self.pages.get()
            if self.empty_page and page <= self.empty_page:
                return
            url = self.url % (*self.active_rank, page)
            try:
                async with aiohttp.ClientSession(
                        headers={"X-Riot-Token": self.handler.api_key}
                ) as session:
                    data = await self.endpoint.request(url, session)
                    self.logging.debug(url)
                    if not data:
                        self.empty_page = max(self.empty_page or 0, page)
                        return
                    self.data += data
                    self.next_page += 1
                    await self.pages.put(self.next_page)
            except LimitBlocked as err:
                self.retry_after = datetime.now() + timedelta(seconds=err.retry_after)
            except RatelimitException:
                pass
            except (Non200Exception, NotFoundException) as err:
                self.logging.exception("Fetch error")
            except Exception as err:
                self.logging.exception('General Exception in Fetch')
            finally:
                await self.pages.put(page)

    async def runner(self):
        """Runner."""
        while not self.handler.is_shutdown:
            if not self.running:
                await asyncio.sleep(5)
                continue
            self.active_rank = await self.rankmanager.get_next()
            self.next_page = 10
            self.empty_page = False
            for i in range(1, 11):
                await self.pages.put(i)
            self.data = []
            try:
                self.logging.debug("START %s %s.", *self.active_rank)
                await asyncio.gather(*[asyncio.create_task(self.worker()) for _ in range(10)])
                self.logging.debug("DONE %s %s.", *self.active_rank)
            except asyncio.CancelledError:
                return
            await self.update_data()
            self.logging.debug("INSERTED %s %s.", *self.active_rank)

            await self.rankmanager.update(key=self.active_rank)

    async def update_data(self):
        "Update all changed users in the DB."
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
                )
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
                await connection.executemany(
                    """INSERT INTO %s.ranking (summoner_id, rank, division, leaguepoints)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (summoner_id) DO 
                        UPDATE SET rank = EXCLUDED.rank,
                                   division = EXCLUDED.division,
                                   leaguepoints = EXCLUDED.leaguepoints,
                                   defunct = FALSE 
                    """
                    % self.name.lower(),
                    to_update,
                )
                self.logging.info(
                    "Updated %s users in %s %s.", len(to_update), *self.active_rank
                )
            except Exception as err:
                self.logging.error(err)
