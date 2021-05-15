"""Match History updater. Pulls matchlists for all player."""
import asyncio
import json
import logging
import traceback
from datetime import datetime, timedelta

import aiohttp
import settings
from connection_manager.buffer import RedisConnector
from connection_manager.persistent import PostgresConnector
from exceptions import RatelimitException, NotFoundException, Non200Exception


class Service:
    """Core service worker object."""

    queues = None

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("MatchTimeline")
        level = logging.INFO
        if settings.DEBUG:
            level = logging.DEBUG
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [MatchTimeline] %(message)s")
        )
        self.logging.addHandler(handler)

        self.task_backlog = []

        self.redis = RedisConnector()
        self.db = PostgresConnector(user=settings.SERVER)
        self.db.set_prepare(self.prepare)

        self.stopped = False
        self.retry_after = datetime.now()
        self.url = (
            f"http://{settings.SERVER}.api.riotgames.com/lol/"
            + "match/v4/timelines/by-match/%s"
        )
        self.buffered_elements = (
            {}
        )  # Short term buffer to keep track of currently ongoing requests

        self.active_tasks = []

    def shutdown(self):
        """Called on shutdown init."""
        self.stopped = True

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
                update_match_sets.append((int(match[0]), json.dumps(timeline)))
            if update_match_sets:
                async with self.db.get_connection() as db:
                    async with db.transaction():
                        await self.match_data_update.executemany(update_match_sets)
            self.logging.info("Inserted %s match_timelines.", len(update_match_sets))

        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

    async def get_task(self):
        """Return tasks to the async worker."""
        async with self.redis.get_connection() as buffer:
            if not (
                task := await buffer.spop(
                    "%s_match_timeline_tasks" % settings.SERVER, settings.BATCH_SIZE
                )
            ):
                return task
            if self.stopped:
                return
            start = int(datetime.utcnow().timestamp())
            await buffer.zadd(
                "%s_match_timeline_in_progress" % settings.SERVER, start, task
            )
            return task

    async def worker(self, matchId, session, delay) -> list:
        """Execute calls until the ratelimit is reached or the internal buffer overflows."""
        await asyncio.sleep(0.8 / settings.BATCH_SIZE * delay)
        while not self.stopped:
            if datetime.now() < self.retry_after:
                delay = max(0.5, (self.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                return [
                    matchId,
                    await self.fetch(session=session, url=self.url % matchId),
                ]
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
            async with aiohttp.ClientSession() as session:
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
        try:
            async with session.get(url, proxy=settings.PROXY_URL) as response:
                await response.text()
        except aiohttp.ClientConnectionError:
            raise Non200Exception()
        if response.status in [429, 430]:
            if response.status == 430:
                if "Retry-At" in response.headers:
                    self.retry_after = datetime.strptime(
                        response.headers["Retry-At"], "%Y-%m-%d %H:%M:%S.%f"
                    )
            elif response.status == 429:
                self.logging.info(response.status)
                delay = 1
                if "Retry-After" in response.headers:
                    delay = int(response.headers["Retry-After"])
                self.retry_after = datetime.now() + timedelta(seconds=delay)
            raise RatelimitException()
        if response.status == 404:
            raise NotFoundException()
        if response.status != 200:
            raise Non200Exception()
        return await response.json(content_type=None)

    async def run(self):
        """
        Runner.
        """
        await self.redis.create_lock()
        await self.async_worker()
