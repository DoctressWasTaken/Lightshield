import asyncio
import logging
from datetime import datetime

import aiohttp

from lightshield.services.league_ranking.rank_manager import RankManager
from lightshield.services.league_ranking import queries


class Service:
    empty_page = False
    active_rank = None

    def __init__(self, name, config, handler):
        self.name = name
        self.logging = logging.getLogger("%s" % name)
        self.handler = handler
        self.database = config.database
        self.rankmanager = RankManager(config, self.logging, handler)
        self.retry_after = datetime.now()
        self.url = (
            f"{handler.protocol}://{self.name.lower()}.api.riotgames.com/lol/"
            + "league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        )
        self.preset = {}
        self.to_update = {}

    async def fetch(self, page, session):
        """Handle request."""

        url = self.url % (*self.active_rank, page)
        async with session.get(url, proxy=self.handler.proxy) as response:
            try:
                data = await response.json()
            except aiohttp.ContentTypeError:
                return page
        if not response.status == 200:
            if response.status == 430:
                wait_until = datetime.fromtimestamp(data["Retry-At"])
                seconds = (wait_until - datetime.now()).total_seconds()
                seconds = max(0.1, seconds)
                await asyncio.sleep(seconds)
            elif response.status == 429:
                await asyncio.sleep(0.5)
            return page
        self.logging.debug(url)
        if not data:
            self.empty_page = True
            return None
        for entry in data:
            rank = [entry["tier"], entry["rank"], entry["leaguePoints"]]
            if (
                entry["summonerId"] not in self.preset
                or self.preset[entry["summonerId"]] != rank
            ):
                self.to_update[entry["summonerId"]] = rank

    async def run(self):
        """Runner."""
        await self.rankmanager.init()
        while not self.handler.is_shutdown:
            self.active_rank = await self.rankmanager.get_next()
            self.to_update = {}
            await self.get_preset()
            pages = list(range(1, 6))
            next_page = 6
            self.empty_page = False
            async with aiohttp.ClientSession() as session:
                while pages and not self.handler.is_shutdown:
                    tasks = [
                        asyncio.create_task(self.fetch(page, session)) for page in pages
                    ]
                    pages = list(filter(None, await asyncio.gather(*tasks)))

                    if not self.empty_page:
                        while len(pages) < 5:
                            pages.append(next_page)
                            next_page += 1

            if self.to_update:
                await asyncio.gather(asyncio.sleep(1), self.update_data())
            # Insert data
            await self.rankmanager.update(key=self.active_rank)

    async def get_preset(self):
        """Get the already existing data."""
        self.preset = {}
        try:
            async with self.handler.db.acquire() as connection:
                if latest := await connection.fetch(
                    queries.preexisting[self.database].format(
                        platform_lower=self.name.lower(),
                        schema=self.handler.connection.schema,
                    ),
                    *self.active_rank,
                ):
                    self.preset = {
                        line["summoner_id"]: [
                            line["rank"],
                            line["division"],
                            line["leaguepoints"],
                        ]
                        for line in latest
                    }
        except Exception as err:
            self.logging.error(err)
            raise err

    async def update_data(self):
        """Update all changed users in the DB."""
        try:
            async with self.handler.db.acquire() as connection:
                to_update_list = [[key] + val for key, val in self.to_update.items()]
                del self.to_update
                updated = len(to_update_list)
                batch = to_update_list[:5000]
                while to_update_list and not self.handler.is_shutdown:
                    try:
                        prep = await connection.prepare(
                            queries.update[self.handler.connection.type].format(
                                platform=self.name,
                                platform_lower=self.name.lower(),
                                schema=self.handler.connection.schema,
                            )
                        )
                        await prep.executemany(batch)
                    except asyncio.exceptions.TimeoutError:
                        self.logging.error("Inserting entries timed out")
                        continue
                    if len(to_update_list) > 5000:
                        to_update_list = to_update_list[5000:]
                    else:
                        to_update_list = []
                    batch = to_update_list[:5000]
                self.logging.info(
                    "Updated %s users in %s %s.",
                    updated - len(to_update_list),
                    *self.active_rank,
                )
        except Exception as err:
            self.logging.error(err)
            raise err
