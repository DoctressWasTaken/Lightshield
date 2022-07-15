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
        try:
            async with session.get(url, proxy=self.handler.proxy) as response:
                data = await response.json()
            match response.status:
                case 200:
                    if not data:
                        self.empty_page = True
                    else:
                        for entry in data:
                            rank = [entry["tier"], entry["rank"], entry["leaguePoints"]]
                            if (
                                entry["summonerId"] not in self.preset
                                or self.preset[entry["summonerId"]]
                                != entry["leaguePoints"]
                            ):
                                self.to_update[entry["summonerId"]] = rank
                    self.logging.debug(url)
                    return None
                case 430:
                    wait_until = datetime.fromtimestamp(data["Retry-At"])
                    seconds = (wait_until - datetime.now()).total_seconds()
                    seconds = max(0.1, seconds)
                    await asyncio.sleep(seconds)
                    return page
                case 429:
                    await asyncio.sleep(0.5)
                    return page
                case _:
                    return page
        except aiohttp.ContentTypeError:
            return page

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
                latest = await connection.fetch(
                    queries.preexisting[self.database].format(
                        platform_lower=self.name.lower(),
                        platform=self.name,
                        schema=self.handler.connection.schema,
                    ),
                    *self.active_rank,
                )
            if latest:
                self.preset = {
                    line["summoner_id"]: line["leaguepoints"] for line in latest
                }
            del latest
        except Exception as err:
            self.logging.error(err)
            raise err

    async def update_data(self):
        """Update all changed users in the DB."""
        try:
            to_update_list = [[key] + val for key, val in self.to_update.items()]
            del self.to_update
            updated = len(to_update_list)
            batch = to_update_list[:5000]
            async with self.handler.db.acquire() as connection:
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
            del to_update_list
        except Exception as err:
            self.logging.error(err)
            raise err
