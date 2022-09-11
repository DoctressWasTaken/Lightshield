import asyncio
import logging
from datetime import datetime

import aiohttp

from lightshield.services.league_ranking import queries
from lightshield.services import ServiceTemplate
from lightshield.services import fail_loop


class LeagueRankingService(ServiceTemplate):
    empty_page = False
    active_rank = None
    is_shutdown = False
    proxy = None
    db = None

    def __init__(self, platform, config):
        super().__init__(platform, config)

        self.retry_after = datetime.now()
        self.url = (
            f"{self.config.proxy.protocol}://{self.platform.lower()}.api.riotgames.com/lol/"
            + "league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        )
        self.preset = {}
        self.to_update = {}
        self.user_rankings = {}

    async def worker(self, rank, division, pages, session):
        """Iterate over pages pulled from a queue until an empty one is found."""
        try:
            while not self.is_shutdown:
                page = pages.get_nowait()
                url = self.url % (rank, division, page)
                async with session.get(url, proxy=self.config.proxy.string) as response:
                    try:
                        api_response = await response.json()
                    except aiohttp.ContentTypeError:
                        pages.task_done()
                        await pages.put(page)
                        continue
                match response.status:
                    case 200:
                        if not api_response:
                            pages.task_done()
                            return
                        for player in api_response:
                            self.user_rankings[player["summonerId"]] = player[
                                "leaguePoints"
                            ]
                        await pages.put(page + 10)
                    case 430:
                        wait_until = datetime.fromtimestamp(api_response["Retry-At"])
                        seconds = (wait_until - datetime.now()).total_seconds()
                        seconds = max(0.1, seconds)
                        await asyncio.sleep(seconds)
                        await pages.put(page)
                    case 429:
                        await asyncio.sleep(0.5)
                        await pages.put(page)
                    case _:
                        await pages.put(page)
                pages.task_done()
        except asyncio.QueueEmpty as err:
            self.logging.info(err)
            return
        except Exception as err:
            self.logging.info(err)

    async def init_ranks(self):
        all_ranks = []
        for platform in self.config.platform_templates:
            for rank in self.config.ranks:
                if rank in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                    all_ranks.append([platform, rank, "I"])
                    continue
                for division in self.config.divisions:
                    all_ranks.append([platform, rank, division])
        async with self.db.acquire() as connection:
            prep = await connection.prepare(
                """
                INSERT INTO rank_distribution (platform, rank, division)
                VALUES($1, $2, $3)
                ON CONFLICT DO NOTHING
            """
            )
            await fail_loop(prep.executemany, [all_ranks], self.logging)

    async def process_division(self, rank, division):
        self.logging.debug("Started %s %s", rank, division)
        pages = asyncio.Queue()
        for i in range(10):
            await pages.put(i + 1)

        async with aiohttp.ClientSession() as session:
            workers = [
                asyncio.create_task(self.worker(rank, division, pages, session))
                for _ in range(5)
            ]
            async with self.db.acquire() as connection:
                preexisting = await connection.fetch(
                    queries.preexisting.format(
                        platform_lower=self.platform.lower(),
                        platform=self.platform,
                    ),
                    rank,
                    division,
                )
            await asyncio.gather(*workers)
            for player in preexisting:
                if (
                    player["summoner_id"] in self.user_rankings
                    and player["leaguepoints"]
                    == self.user_rankings[player["summoner_id"]]
                ):
                    del self.user_rankings[player["summoner_id"]]

            async with self.db.acquire() as connection:
                prep = await connection.prepare(
                    queries.update.format(
                        platform=self.platform,
                        platform_lower=self.platform.lower(),
                    )
                )
                await fail_loop(
                    prep.executemany,
                    [
                        [
                            [key, rank, division, value]
                            for key, value in self.user_rankings.items()
                        ]
                    ],
                    self.logging,
                )

        self.logging.debug("Done %s %s", rank, division)

    async def run(self, db):
        """Runner."""
        self.db = db
        await self.init_ranks()
        while not self.is_shutdown:
            async with self.db.acquire() as connection:
                next = await connection.fetchrow(
                    """
                    SELECT rank, division
                    FROM rank_distribution
                    WHERE platform = $1
                    AND last_updated IS NULL OR last_updated < NOW() - '{cycle_length:0f} hours'::INTERVAL
                    ORDER BY last_updated NULLS FIRST
                    LIMIT 1
                """.format(
                        cycle_length=self.config.services.league_ranking.cycle_length
                    ),
                    self.platform,
                )
            if not next:
                for _ in range(30):
                    if self.is_shutdown:
                        return
                    await asyncio.sleep(2)
                continue
            await self.process_division(next["rank"], next["division"])
            async with self.db.acquire() as connection:
                await connection.fetchrow(
                    """
                    UPDATE rank_distribution
                    SET last_updated = NOW()
                    WHERE rank = $1
                        AND division = $2
                        AND platform = $3
                        """,
                    next["rank"],
                    next["division"],
                    self.platform,
                )
