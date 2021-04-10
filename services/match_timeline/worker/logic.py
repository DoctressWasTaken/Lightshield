"""Match History updater. Pulls matchlists for all player."""
import asyncio
import json
import logging
import os
import traceback
from datetime import datetime, timedelta

import aiohttp
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
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [MatchTimeline] %(message)s")
        )
        self.logging.addHandler(handler)

        self.proxy = os.environ["PROXY_URL"]
        self.server = os.environ["SERVER"]
        self.batch_size = int(os.environ["BATCH_SIZE"])

        self.redis = RedisConnector()
        self.db = PostgresConnector(user=self.server.lower())
        self.db.set_prepare(self.prepare)

        self.stopped = False
        self.retry_after = datetime.now()
        self.url = (
            f"http://{self.server.lower()}.api.riotgames.com/lol/"
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
        self.match_update = await conn.prepare(
            """
        UPDATE %s.match
            SET timeline_pulled = TRUE
            WHERE match_id = $1
        """
            % self.server.lower()
        )
        self.match_data_update = await conn.prepare(
            """
            INSERT INTO %s.match_data (match_id, timeline)
            VALUES ($1, $2)
            ON CONFLICT (match_id) DO UPDATE
            SET timeline = EXCLUDED.timeline
            """
            % self.server.lower()
        )

    async def flush_manager(self, match_timelines):
        """Update entries in postgres once enough tasks are done."""
        try:
            update_match_sets = []
            update_match_data_sets = []
            for match in match_timelines:
                if not match[1]:
                    continue
                timeline = match[1]
                # Team Details
                update_match_sets.append((int(match[0]),))
                update_match_data_sets.append(
                    (
                        int(match[0]),
                        json.dumps(timeline),
                    )
                )
            if update_match_sets:
                async with self.db.get_connection() as db:
                    await self.match_data_update.executemany(update_match_data_sets)
                    await self.match_update.executemany(update_match_sets)
            self.logging.info("Inserted %s match_timelines.", len(update_match_sets))

        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)

    async def get_task(self):
        """Return tasks to the async worker."""
        async with self.redis.get_connection() as buffer:
            if not (
                tasks := await buffer.spop(
                    "%s_match_timeline_tasks" % self.server, self.batch_size
                )
            ):
                return tasks
            if self.stopped:
                return
            start = int(datetime.utcnow().timestamp())
            for entry in tasks:
                await buffer.zadd(
                    "%s_match_timeline_in_progress" % self.server, start, entry
                )
            return tasks

    async def worker(self, matchId, session, delay) -> list:
        """Multiple started per separate processor.
        Does calls continuously until it reaches an empty page."""
        await asyncio.sleep(0.8 / self.batch_size * delay)
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
        while not self.stopped:
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
            await self.flush_manager(results)
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
            async with session.get(url, proxy=self.proxy) as response:
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
        await self.async_worker()
