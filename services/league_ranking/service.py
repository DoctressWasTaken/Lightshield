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

    def __init__(self, name, id, handler):
        self.name = name
        self.id = id
        self.logging = logging.getLogger("%s" % name)
        self.handler = handler
        self.rankmanager = RankManager(self.logging)
        self.retry_after = datetime.now()
        self.current_pages = []
        self.data = []
        self.url = (
            f"https://{self.id}.api.riotgames.com/lol/"
            + "league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        )

    async def init(self):
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

    async def runner(self):
        """Runner."""
        while not self.handler.is_shutdown:
            if not self.running:
                await asyncio.sleep(5)
                continue
            tier, division = await self.rankmanager.get_next()

            self.next_page = 10
            self.current_pages = [_ for _ in range(1, 11)]
            self.empty_page = False
            self.data = []
            self.logging.info("Commencing on %s %s.", tier, division)
            await self.get_data(tier, division)
            self.logging.info("Calls done %s %s.", tier, division)
            await self.update_data(tier, division)
            self.logging.info("Data update done %s %s.", tier, division)

            await self.rankmanager.update(key=(tier, division))

    async def get_data(self, tier, division):
        """Request data from the API."""
        async with aiohttp.ClientSession(
            headers={"X-Riot-Token": self.handler.api_key}
        ) as session:
            while self.current_pages:
                if self.handler.is_shutdown:
                    return
                if not self.running:
                    await asyncio.sleep(5)
                    continue
                while (
                    delay := (self.retry_after - datetime.now()).total_seconds()
                ) > 0:
                    await asyncio.sleep(min(0.1, delay))

                results = await asyncio.gather(
                    *[
                        asyncio.create_task(
                            self.fetch(session, page, self.url % (tier, division, page))
                        )
                        for page in self.current_pages
                    ]
                )
                self.current_pages = [page for page in results if page]
                while not self.empty_page and len(self.current_pages) < 10:
                    self.next_page += 1
                    self.current_pages.append(self.next_page)

    async def fetch(self, session, page, url):
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
            data = await self.endpoint.request(url, session)
            self.logging.debug(url)
            if not data:
                self.empty_page = True
                return
            self.data += data
        except LimitBlocked as err:
            self.retry_after = datetime.now() + timedelta(seconds=err.retry_after)
            return page
        except (RatelimitException, Non200Exception, NotFoundException) as err:
            self.logging.error("Others")
            return page
        except Exception as err:
            self.logging.error(err)

    async def update_data(self, tier, division):
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
                    tier,
                    division,
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
                for new in self.data:
                    rank = [new["tier"], new["rank"], new["leaguePoints"]]
                    if new["summonerId"] not in preset:
                        to_update.append([new["summonerId"]] + rank)
                    elif preset[new["summonerId"]] != rank:
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
                self.logging.info("Updated %s users.", len(to_update))
            except Exception as err:
                self.logging.error(err)
