import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp


class Platform:
    running = False
    _runner = None

    def __init__(self, region, platform, config, handler):
        self.region = region
        self.platform = platform
        self.handler = handler
        self.logging = logging.getLogger("%s" % platform)
        self.service = config.services.match_history

        self.proxy = handler.proxy
        self.endpoint_url = (
            f"{config.connections.proxy.protocol}://{self.region.lower()}.api.riotgames.com"
            f"/lol/match/v5/matches/by-puuid/%s/ids"
            f"?count=100"
        )
        if self.service.type:
            self.endpoint_url += "&type=%s" % self.service.type
        if self.service.queue:
            self.endpoint_url += "&queue=%s" % self.service.queue

        self.updated_players = []

    async def run(self):
        while not self.handler.is_shutdown:
            async with self.handler.postgres.acquire() as connection:
                async with connection.transaction():
                    players = await connection.fetch(
                        """
                        SELECT  puuid, 
                                latest_match, 
                                last_history_update
                        FROM summoner
                        WHERE platform = $1
                            AND (
                                last_activity > last_history_update
                                OR last_history_update IS NULL)
                            AND (last_history_update + INTERVAL '1 days' * $2 < CURRENT_DATE
                            OR last_history_update IS NULL)
                        ORDER BY last_history_update NULLS FIRST
                        LIMIT 200
                        FOR UPDATE 
                        SKIP LOCKED
                    """,
                        self.platform,
                        self.service.min_wait,
                    )
                    if not players:
                        await asyncio.sleep(5)
                        continue
                    async with aiohttp.ClientSession() as session:
                        matches = await asyncio.gather(
                            *[
                                asyncio.create_task(self.worker(player, session))
                                for player in players
                            ]
                        )
                    if self.handler.is_shutdown:
                        return
                    set_matches = list(set([m for mm in matches for m in mm]))
                    if set_matches:
                        by_platform = {}
                        for match in set_matches:
                            platform, id = match.split("_")
                            if platform not in by_platform:
                                by_platform[platform] = []
                            by_platform[platform].append(int(id))

                        for platform in by_platform.keys():
                            if self.service.queue:
                                data = [
                                    [platform, id, self.service.queue]
                                    for id in by_platform[platform]
                                ]
                                await connection.executemany(
                                    """
                                    INSERT INTO "match_{platform:s}" (platform, match_id, queue)
                                    VALUES ($1, $2, $3)
                                    ON CONFLICT DO NOTHING
                                """.format(
                                        platform=platform.lower()
                                    ),
                                    data,
                                )
                            else:
                                data = [[platform, id] for id in by_platform[platform]]
                                await connection.executemany(
                                    """
                                    INSERT INTO "match_{platform:s}" (platform, match_id)
                                    VALUES ($1, $2)
                                    ON CONFLICT DO NOTHING
                                """.format(
                                        platform=platform.lower()
                                    ),
                                    data,
                                )
                    if self.updated_players:
                        prep = await connection.prepare(
                            """
                            UPDATE summoner
                            SET latest_match = $2,
                                last_history_update = $3
                            WHERE puuid = $1
                        """
                        )
                        await prep.executemany(self.updated_players)
                        self.logging.info(
                            "Updated %s users [%s matches]",
                            len(self.updated_players),
                            len(set_matches),
                        )
                    self.updated_players = []

    async def worker_calls(self, session, task):
        url, page = task
        while not self.handler.is_shutdown:
            try:
                async with session.get(url, proxy=self.proxy) as response:
                    if response.status == 200:
                        return [page, await response.json()]
                    if response.status == 404:
                        return [page, []]
                    if response.status == 429:
                        await asyncio.sleep(0.5)
                    elif response.status == 430:
                        data = await response.json()
                        wait_until = datetime.fromtimestamp(data["Retry-At"])
                        seconds = (wait_until - datetime.now()).total_seconds()
                        seconds = max(0.1, seconds)
                        await asyncio.sleep(seconds)
                    else:
                        await asyncio.sleep(0.1)
            except aiohttp.ClientProxyConnectionError:
                await asyncio.sleep(0.1)
                continue

    async def worker(self, player, session):
        while not self.handler.is_shutdown:
            now = datetime.now() - timedelta(days=self.service.history.days)
            now_tst = int(now.timestamp())
            url = self.endpoint_url % player["puuid"]
            url += "&startTime=%s" % now_tst
            tasks = []
            page = 0
            while page * 100 < self.service.history.matches:
                tasks.append([url + "&start=%s" % page, page])
                page += 1
            results = await asyncio.gather(
                *[self.worker_calls(session, task) for task in tasks]
            )
            if len(results) == 0:
                continue
            pages_sorted = {page[0]: page[1] for page in results}
            matches = []
            for page in range(len(pages_sorted)):
                matches += pages_sorted[page]
            new_latest = None
            if matches:
                new_latest = int(matches.pop().split("_")[1])
            self.updated_players.append([player["puuid"], new_latest, now])
            return matches
