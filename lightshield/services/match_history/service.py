import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp


class Platform:
    running = False
    _runner = None

    def __init__(self, region, platforms, config, handler):
        self.region = region
        self.platforms = platforms
        self.handler = handler
        self.logging = logging.getLogger("%s" % region)
        self.service = config.services.match_history
        self.mapping = getattr(config.statics.mapping, region)

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
                    players = await connection.fetch("""
                        SELECT  puuid, 
                                latest_match, 
                                last_history_update
                        FROM summoner
                        WHERE platform = ANY($1::platform[])
                            AND (
                                last_activity > last_history_update
                                OR last_history_update IS NULL)
                        ORDER BY last_history_update NULLS FIRST
                        LIMIT 5 
                        FOR UPDATE 
                        SKIP LOCKED
                    """, self.mapping)
                    async with aiohttp.ClientSession() as session:
                        matches = await asyncio.gather(*[
                            asyncio.create_task(self.worker(player, session))
                            for player in players
                        ])
                    if self.handler.is_shutdown:
                        return
                    set_matches = list(set([m for mm in matches for m in mm]))
                    if set_matches:
                        if self.service.queue:
                            reformated = [(p[0], int(p[1]), self.service.queue) for p in
                                          [p.split('_') for p in set_matches]]
                            await connection.executemany("""
                                INSERT INTO %s.match (platform, match_id, queue)
                                VALUES ($1, $2, $3)
                                ON CONFLICT DO NOTHING
                            """ % self.region, reformated)
                        else:
                            reformated = [(p[0], int(p[1])) for p in
                                          [p.split('_') for p in set_matches]]
                            await connection.executemany("""
                                INSERT INTO %s.match (platform, match_id)
                                VALUES ($1, $2)
                                ON CONFLICT DO NOTHING
                            """ % self.region, reformated)
                    if self.updated_players:
                        prep = await connection.prepare("""
                            UPDATE summoner
                            SET latest_match = $2,
                                last_history_update = $3
                            WHERE puuid = $1
                        """)
                        await prep.executemany(self.updated_players)

    async def worker_calls(self, session, task):
        url, page = task
        while not self.handler.is_shutdown:
            async with session.get(url, proxy=self.proxy) as response:
                if response.status == 200:
                    return [page, await response.json()]
                if response.status == 404:
                    return [page, []]
                if response.status == 429:
                    await asyncio.sleep(0.5)
                elif response.status == 430:
                    data = response.json()
                    wait_until = datetime.fromtimestamp(data["Retry-At"])
                    seconds = (wait_until - datetime.now()).total_seconds()
                    seconds = max(0.1, seconds)
                    await asyncio.sleep(seconds)
                else:
                    await asyncio.sleep(0.1)

    async def worker(self, player, session):
        while not self.handler.is_shutdown:
            now = datetime.now() - timedelta(days=self.service.history.days)
            now_tst = int(now.timestamp())
            url = self.endpoint_url % player['puuid']
            url += '&startTime=%s' % now_tst
            tasks = []
            page = 0
            while page * 100 < self.service.history.matches:
                tasks.append([url + "&start=%s" % page, page])
                page += 1
            results = await asyncio.gather(*[
                self.worker_calls(session, task)
                for task in tasks
            ])

            pages_sorted = {page[0]: page[1] for page in results}
            matches = []
            for page in range(len(pages_sorted)):
                matches += pages_sorted[page]
            new_latest = None
            if matches:
                new_latest = int(matches.pop().split("_")[1])
            self.updated_players.append(
                [player['puuid'],
                 new_latest,
                 now
                 ]
            )
            return matches

    # async def user_handler(self, session, task):
#
# async def task_updater(self):
#    """Pull new tasks when the list is empty."""
#    while True:
#        if len(self.result_matchids) >= 800:
#            matches = list(set(self.result_matchids))
#            self.result_matchids = []
#            summoners = self.result_summoners.copy()
#            self.result_summoners = []
#            await self.flush_tasks(matches, summoners)
#        if not self.running:
#            await asyncio.sleep(5)
#            continue
#        if len(self.tasks) > 50:
#            await asyncio.sleep(5)
#            continue
#        try:
#            async with self.handler.postgres.acquire() as connection:
#                entries = await connection.fetch(
#                    """UPDATE summoner
#                            SET reserved_match_history = current_date + INTERVAL '10 minute'
#                            WHERE puuid IN (
#                                SELECT puuid
#                                FROM summoner
#                                WHERE last_platform = any($1::platform[])
#                                ORDER BY CASE WHEN last_updated IS NULL THEN 0 ELSE 1 END, last_updated
#                                LIMIT $2
#                                FOR UPDATE
#                            )
#                            RETURNING puuid, last_updated, last_match
#                    """,
#                    self.platforms,
#                    200,
#                )
#                self.logging.debug(
#                    "Refilling tasks [%s -> %s].",
#                    len(self.tasks),
#                    len(self.tasks) + len(entries),
#                )
#                if len(entries) == 0:
#                    await asyncio.sleep(30)
#                    matches = list(set(self.result_matchids))
#                    self.result_matchids = []
#                    summoners = self.result_summoners.copy()
#                    self.result_summoners = []
#                    await self.flush_tasks(matches, summoners)
#
#                self.tasks += entries
#        except Exception as err:
#            self.logging.error(err)
#
# async def fetch(self, target, start, session):
#    """Call and handle response."""
#    url = self.endpoint_url % (target, start)
#    try:
#        data = await self.endpoint.request(url, session)
#        self.result_matchids += data
#        if start == 0:
#            platform, id = data[0].split("_")
#            id = int(id)
#            self.result_summoners.append([platform, id, target])
#        self.logging.debug(url)
#    except LimitBlocked as err:
#        self.retry_after = datetime.now() + timedelta(seconds=err.retry_after)
#        return start
#    except RatelimitException as err:
#        self.logging.error("Ratelimit")
#        return start
#    except Non200Exception as err:
#        self.logging.exception("Non 200 Exception")
#        return start
#    except NotFoundException:
#        self.logging.error("Not found error.")
#    except Exception as err:
#        self.logging.exception(err)
#        return start
#
# async def update_full(self, target):
#    """Update 10 pages for a user."""
#    try:
#        starts = [100 * i for i in range(10)]
#        async with aiohttp.ClientSession(
#                headers={"X-Riot-Token": self.handler.api_key}
#        ) as session:
#            while starts:
#                while (
#                        delay := (self.retry_after - datetime.now()).total_seconds()
#                ) > 0:
#                    await asyncio.sleep(min(0.1, delay))
#                starts = [
#                    start
#                    for start in (
#                        await asyncio.gather(
#                            *[
#                                asyncio.create_task(
#                                    self.fetch(target["puuid"], start, session)
#                                )
#                                for start in starts
#                            ]
#                        )
#                    )
#                    if start
#                ]
#    except Exception as err:
#        self.logging.error("FULL: %s", err)
#
# async def update_single(self, target):
#    """Update a single page for a user."""
#    offset = 0
#    new_last = None
#    new_matches = []
#    try:
#        async with aiohttp.ClientSession(
#                headers={"X-Riot-Token": self.handler.api_key}
#        ) as session:
#            while True:
#                while (
#                        delay := (self.retry_after - datetime.now()).total_seconds()
#                ) > 0:
#                    await asyncio.sleep(min(0.1, delay))
#                url = self.endpoint_url % (target["puuid"], offset)
#                try:
#                    data = await self.endpoint.request(url, session)
#                    if not new_last:
#                        new_last = data[0]
#                    if target["last_match"] in data:
#                        for entry in data:
#                            if entry == target["last_match"]:
#                                break
#                            new_matches.append(entry)
#                        return new_matches
#                    else:
#                        new_matches += data
#                    if new_matches:
#                        self.result_matchids += new_matches
#                    if offset == 900:
#                        return new_matches
#                    offset += 100
#                except LimitBlocked as err:
#                    self.retry_after = datetime.now() + timedelta(
#                        seconds=err.retry_after
#                    )
#                except RatelimitException as err:
#                    self.logging.error("Ratelimit")
#                except Non200Exception as err:
#                    self.logging.error("Others")
#                except NotFoundException:
#                    self.logging.error("Not found error.")
#                except Exception as err:
#                    self.logging.error(err)
#    except Exception as err:
#        self.logging.error(err)
#    finally:
#        platform, id = new_last.split("_")
#        id = int(id)
#        self.result_summoners.append([platform, id, target["puuid"]])
#
# async def worker(self):
#    """Execute requests."""
#    while not self.running:
#        await asyncio.sleep(5)
#    while True:
#        if not self.running:
#            await asyncio.sleep(5)
#            continue
#        if not self.tasks:
#            await asyncio.sleep(5)
#            continue
#        target = self.tasks.pop()
#        if not target["last_match"]:
#            await self.update_full(target)
#            continue
#        await self.update_single(target)
#
# async def flush_tasks(self, matches, summoner):
#    """Insert results from requests into the db."""
#    try:
#        async with self.handler.postgres.acquire() as connection:
#            if matches or summoner:
#                self.logging.info(
#                    "Flushing %s match ids and %s summoner updates.",
#                    len(matches),
#                    len(summoner),
#                )
#            if matches:
#                splits = []
#                for match in list(set(matches)):
#                    platform, id = match.split("_")
#                    splits.append((platform, int(id)))
#                query = await connection.prepare(
#                    """INSERT INTO %s.match (platform, match_id) VALUES ($1, $2)
#                        ON CONFLICT DO NOTHING
#                    """
#                    % self.name,
#                )
#                await query.executemany(splits)
#            if summoner:
#                summoner_cleaned = [[s[0], s[1], s[2]] for s in summoner]
#                query = await connection.prepare(
#                    """UPDATE summoner
#                        SET last_updated = current_timestamp,
#                            last_platform = $1,
#                            last_match = $2,
#                            reserved_match_history = NULL
#                        WHERE puuid = $3
#                    """,
#                )
#                await query.executemany(summoner_cleaned)
#    except Exception as err:
#        self.logging.info(err)
#
