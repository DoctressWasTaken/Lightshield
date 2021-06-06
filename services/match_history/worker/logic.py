"""Match History updater. Pulls matchlists for all player."""
import asyncio
import logging
import os
import traceback
from datetime import datetime, timedelta

import aiohttp
import aioredis
import asyncpg

from lightshield import settings
from lightshield.exceptions import (
    RatelimitException,
    NotFoundException,
    Non200Exception,
    LimitBlocked,
)
from lightshield.proxy import Proxy


class Service:
    """Core service worker object."""

    queues = None

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("MatchHistory")

        # Postgres
        self.db = None
        # Proxy
        self.proxy = Proxy()
        self.endpoint_url = f"https://{settings.SERVER}.api.riotgames.com/lol/match/v4/matchlists/by-account/"
        # Redis
        self.redis = None

        self.stopped = False
        self.retry_after = datetime.now()
        self.url = (
            f"https://{settings.SERVER}.api.riotgames.com/lol/"
            + "match/v4/matchlists/by-account/%s?beginIndex=%s&endIndex=%s"
        )

        if "QUEUES" in os.environ:
            self.queues = [int(queue) for queue in os.environ["QUEUES"].split(",")]
            self.url = self.url + "&queue=" + os.environ["QUEUES"]

        self.buffered_elements = (
            {}
        )  # Short term buffer to keep track of currently ongoing requests

        self.active_tasks = []
        self.insert_query = None

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

    async def init(self):
        self.db = await asyncpg.create_pool(
            host=settings.PERSISTENT_HOST,
            port=settings.PERSISTENT_PORT,
            user=settings.SERVER,
            password=settings.PERSISTENT_PASSWORD,
            database=settings.PERSISTENT_DATABASE,
        )
        self.redis = await aioredis.create_redis_pool(
            (settings.REDIS_HOST, settings.REDIS_PORT), encoding="utf-8"
        )

        await self.proxy.init(settings.PROXY_SYNC_HOST, settings.PROXY_SYNC_PORT)
        self.logging.info(self.endpoint_url)
        self.endpoint = await self.proxy.get_endpoint(self.endpoint_url)

    async def flush_manager(self, matches, account_id, keys):
        """Update entries in postgres once enough tasks are done."""
        try:
            sets = []
            for entry in matches:
                if self.queues and int(entry["queue"]) not in self.queues:
                    continue
                sets.append(
                    (
                        entry["gameId"],
                        entry["queue"],
                        datetime.fromtimestamp(entry["timestamp"] // 1000),
                    )
                )
            async with self.db.acquire() as connection:
                if sets:
                    await connection.executemany(
                        """
                        INSERT INTO %s.match (match_id, queue, timestamp)
                        VALUES ($1, $2, $3)
                        ON CONFLICT DO NOTHING;
                        """
                        % settings.SERVER,
                        sets,
                    )
                    self.logging.info("Inserted %s sets for %s.", len(sets), account_id)

                await connection.execute(
                    """
                    UPDATE %s.summoner
                    SET wins_last_updated = $1,
                        losses_last_updated = $2
                    WHERE account_id = $3
                    """
                    % settings.SERVER,
                    int(keys["wins"]),
                    int(keys["losses"]),
                    account_id,
                )
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

    async def get_task(self):
        """Return tasks to the async worker."""
        while (
            not (
                task := await self.redis.zpopmax(
                    "%s_match_history_tasks" % settings.SERVER, 1
                )
            )
            and not self.stopped
        ):
            await asyncio.sleep(5)
            if self.stopped:
                return
        keys = await self.redis.hgetall(
            "%s:%s:%s" % (settings.SERVER, task[0], task[1])
        )
        await self.redis.delete("%s:%s:%s" % (settings.SERVER, task[0], task[1]))
        start = int(datetime.utcnow().timestamp())
        await self.redis.zadd("match_history_in_progress", start, task[0])
        return [task[0], int(task[1])], keys

    async def full_refresh(self, account_id, keys):
        """Pull match-history data until the page is empty."""
        worker = 6
        async with aiohttp.ClientSession(
            headers={"X-Riot-Token": settings.API_KEY}
        ) as session:
            match_data = await asyncio.gather(
                *[
                    asyncio.create_task(
                        self.worker(
                            account_id, session=session, offset=i, worker=worker
                        )
                    )
                    for i in range(worker)
                ]
            )
        if self.stopped:
            return
        matches = []
        for data in match_data:
            matches += data
        await self.flush_manager(matches, account_id, keys)

    async def partial_refresh(self, account_id, to_call, keys):
        """Pull match-history data corresponding to how much"""
        worker = 3
        pages = to_call // 100 + 1
        async with aiohttp.ClientSession(
            headers={"X-Riot-Token": settings.API_KEY}
        ) as session:
            match_data = await asyncio.gather(
                *[
                    asyncio.create_task(
                        self.worker(
                            account_id,
                            session=session,
                            offset=i,
                            worker=worker,
                            limit=pages,
                        )
                    )
                    for i in range(worker)
                ]
            )
        if self.stopped:
            return
        matches = []
        for data in match_data:
            matches += data
        await self.flush_manager(matches, account_id, keys)

    async def worker(self, account_id, session, offset, worker, limit=None) -> list:
        """Multiple started per separate processor.
        Does calls continuously until it reaches an empty page."""
        matches = []
        start = offset * 100
        end = offset * 100 + 100
        while not self.stopped:
            if limit and start >= limit * 100:
                return [
                    match
                    for match in matches
                    if match["platformId"] == settings.SERVER.upper()
                ]
            while (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(min(0.1, delay))
            try:
                result = await self.fetch(
                    session=session, url=self.url % (account_id, start, end)
                )
                if not result["matches"]:
                    return [
                        match
                        for match in matches
                        if match["platformId"] == settings.SERVER.upper()
                    ]
                matches += result["matches"]
                start += 100 * worker
                end += 100 * worker
                await asyncio.sleep(0.2)
            except LimitBlocked as err:
                self.retry_after = datetime.now() + timedelta(seconds=err.retry_after)

            except NotFoundException:
                return [
                    match
                    for match in matches
                    if match["platformId"] == settings.SERVER.upper()
                ]
            except (Non200Exception, RatelimitException):
                continue
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                self.logging.info(err)
        return []

    async def async_worker(self):
        while not self.stopped:
            if not (data := await self.get_task()):
                continue
            task, keys = data
            if task[1] == 9999:
                await self.full_refresh(task[0], keys)
            else:
                await self.partial_refresh(*task, keys)
            await asyncio.sleep(0.01)

    async def fetch(self, session, url) -> dict:
        """
        Execute call to external target using the proxy server.

        Receives aiohttp session as well as url to be called.
        Executes the request and returns either the content of the
        response as json or raises an exeption depending on response.
        :param session: The aiohttp Clientsession used to execute the call.
        :param url: String url ready to be requested.

        :returns: Request response as dict.
        :raises RatelimitException: on 429 or 430 HTTP Code.
        :raises NotFoundException: on 404 HTTP Code.
        :raises Non200Exception: on any other non 200 HTTP Code.
        """
        return await self.endpoint.request(url, session)

    async def run(self):
        """
        Runner.
        """
        await self.init()
        await self.async_worker()
