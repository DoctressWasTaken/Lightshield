"""Match History updater. Pulls matchlists for all player."""
import asyncio
import json
import logging
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
        self.logging = logging.getLogger("MatchTimeline")

        self.task_backlog = []

        # Postgres
        self.db = None
        # Proxy
        self.proxy = Proxy()
        self.endpoint_url = f"https://{settings.SERVER}.api.riotgames.com/lol/match/v4/timelines/by-match/"
        # Redis
        self.redis = None

        self.stopped = False
        self.retry_after = datetime.now()
        self.url = f"https://{settings.SERVER}.api.riotgames.com/lol/match/v4/timelines/by-match/%s"
        self.buffered_elements = (
            {}
        )  # Short term buffer to keep track of currently ongoing requests

        self.active_tasks = []

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

    async def prepare(self, conn):
        self.match_data_update = await conn.prepare(
            """
            UPDATE %s.match_data
            SET timeline = $1
            WHERE match_id = $2
            """
            % settings.SERVER
        )

    async def flush_manager(self, match_timelines):
        """Update entries in postgres once enough tasks are done."""
        try:
            update_match_sets = []
            for match in match_timelines:
                if not match[1]:
                    continue
                timeline = match[1]
                # Team Details
                update_match_sets.append((json.dumps(timeline), int(match[0])))
            if update_match_sets:
                async with self.db.acquire() as connection:
                    async with connection.transaction():
                        await connection.executemany(
                            """
                                        UPDATE %s.match_data
                                        SET timeline = $1
                                        WHERE match_id = $2
                                        """
                            % settings.SERVER,
                            update_match_sets,
                        )
            self.logging.info("Inserted %s match_timelines.", len(update_match_sets))

        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

    async def get_task(self):
        """Return tasks to the async worker."""
        if not (
            tasks := await self.redis.spop(
                "%s_match_timeline_tasks" % settings.SERVER, settings.BATCH_SIZE
            )
        ):
            return tasks
        if self.stopped:
            return
        start = int(datetime.utcnow().timestamp())
        for entry in tasks:
            await self.redis.zadd(
                "%s_match_timeline_in_progress" % settings.SERVER, start, entry
            )
        return tasks

    async def worker(self, matchId, session, delay) -> list:
        """Execute calls until the ratelimit is reached or the internal buffer overflows."""
        await asyncio.sleep(0.8 / settings.BATCH_SIZE * delay)
        while not self.stopped:
            while (delay := (self.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(min(0.1, delay))
            try:
                return [
                    matchId,
                    await self.fetch(session=session, url=self.url % matchId),
                ]
            except LimitBlocked as err:
                self.retry_after = datetime.now() + timedelta(seconds=err.retry_after)
            except NotFoundException:
                return [matchId, None]
            except (Non200Exception, RatelimitException):
                continue
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                self.logging.info(err)
        return [matchId, None]

    async def async_worker(self):
        afk_alert = False
        flushing_task = None
        while not self.stopped:
            if flushing_task:
                await flushing_task
                flushing_task = None
            if not (tasks := await self.get_task()):
                if not afk_alert:
                    self.logging.info("Found no tasks.")
                    afk_alert = True
                await asyncio.sleep(10)
                continue
            afk_alert = False
            async with aiohttp.ClientSession(
                headers={"X-Riot-Token": settings.API_KEY}
            ) as session:
                results = await asyncio.gather(
                    *[
                        asyncio.create_task(
                            self.worker(matchId=matchId, session=session, delay=index)
                        )
                        for index, matchId in enumerate(tasks)
                    ]
                )
            flushing_task = asyncio.create_task(self.flush_manager(results))
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
