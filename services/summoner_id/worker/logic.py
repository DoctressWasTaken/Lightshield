"""Summoner ID Updater  - Logical elements.

No Service defined as the service is exactly the same as the default case.
Import is done directly.
"""
import traceback

import aiohttp
import aioredis
import asyncio
import asyncpg
import logging
from datetime import datetime, timedelta
from lightshield import settings
from lightshield.exceptions import (
    RatelimitException,
    NotFoundException,
    Non200Exception,
    LimitBlocked
)
from lightshield.proxy import Proxy


class Service:
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        self.logging = logging.getLogger("SummonerIDs")
        self.url = (
                f"https://{settings.SERVER}.api.riotgames.com/lol/"
                + "summoner/v4/summoners/%s"
        )
        self.stopped = False
        self.retry_after = datetime.now()
        # Postgres
        self.db = None
        # Proxy
        self.proxy = Proxy()
        self.endpoint_url = f"https://{settings.SERVER}.api.riotgames.com/lol/summoner/v4/summoners/"
        # Redis
        self.redis = None

        self.completed_tasks = []
        self.to_delete = []

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
            (settings.BUFFER_HOST, settings.BUFFER_PORT), encoding="utf-8"
        )

        await self.proxy.init(settings.PROXY_SYNC_HOST, settings.PROXY_SYNC_PORT)
        self.logging.info(self.endpoint_url)
        self.endpoint = await self.proxy.get_endpoint(self.endpoint_url)

    async def insert(self, batch):
        """Update entry in postgres."""
        async with self.db.acquire() as connection:
            await connection.executemany(
                """
                UPDATE %s.summoner
                SET account_id = $1, puuid = $2
                WHERE summoner_id = $3;
                """
                % settings.SERVER,
                batch)

    async def drop(self, summoner_id):
        async with self.db.acquire() as connection:
            await connection.execute(
                """
                    DELETE FROM %s.summoner
                    WHERE summoner_id = $1;
                    """,
                summoner_id,
            )

    async def get_task(self):
        """Return tasks to the async worker."""
        task = await self.redis.spop("%s_summoner_id_tasks" % settings.SERVER)
        if not task or self.stopped:
            return
        start = int(datetime.utcnow().timestamp())
        await self.redis.zadd(
            "%s_summoner_id_in_progress" % settings.SERVER, start, task
        )
        return task

    async def logic(self, session, summoner_id):
        failed = summoner_id
        while failed:
            if datetime.now() < self.retry_after:
                delay = max(0.5, (self.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                summoner_id = failed
                failed = None
                response = await self.fetch(session, self.url % summoner_id)
                return [response["accountId"], response["puuid"], summoner_id]
            except LimitBlocked as err:
                self.retry_after = datetime.now() + timedelta(
                    seconds=err.retry_after
                )
            except NotFoundException:
                await self.drop(summoner_id)
            except (RatelimitException, Non200Exception):
                failed = summoner_id
                continue
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                self.logging.info(err)
            await asyncio.sleep(0.01)

    async def async_worker(self):
        """Create only a new call if the summoner is not yet in the db."""
        self.logging.info("Initiated.")
        failed = None
        while not self.stopped:
            batch = []
            async with aiohttp.ClientSession(headers={'X-Riot-Token': settings.API_KEY}) as session:
                for _ in range(50):
                    summoner_id = await self.get_task()
                    if not summoner_id:
                        break
                    else:
                        batch.append(
                            asyncio.create_task(self.logic(session, summoner_id))
                        )
                if not batch:
                    await asyncio.sleep(60)
                    continue
                batch_results = await asyncio.gather(*batch)
            self.logging.info("Inserted %s summoner_ids.", len(batch))
            await self.insert(batch_results)

        self.logging.info("Stopped worker.")

    async def fetch(self, session, url):
        """Execute call to external target using the proxy server.

        Receives aiohttp session as well as url to be called. Executes the request and returns
        either the content of the response as json or raises an exeption depending on response.
        :param session: The aiohttp Clientsession used to execute the call.
        :param url: String url ready to be requested.

        :returns: Request response as dict.

        :raises RatelimitException: on 429 or 430 HTTP Code.
        :raises NotFoundException: on 404 HTTP Code.
        :raises Non200Exception: on any other non 200 HTTP Code.
        """
        return await self.endpoint.request(url, session)

    async def run(self):
        """Runner."""
        await self.init()
        await self.async_worker()
