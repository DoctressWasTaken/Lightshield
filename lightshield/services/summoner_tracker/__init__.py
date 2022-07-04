"""Check summoner that hasn't been recognized as active for server changes."""
import asyncio
import logging
import postgres
import asyncpg
import uvloop
from datetime import datetime
import os
import aiohttp

uvloop.install()


class Account:

    def __init__(self, data):
        self.puuid = data['puuid']
        self.platform = data['platform']
        self.activity = data['last_activity']
        self.updated = False

    def add_result(self, platform, activity):
        new_activity = datetime.fromtimestamp(activity / 1000)
        if self.activity > new_activity:
            return
        self.updated = True
        self.activity = new_activity
        self.platform = platform

    def result(self):
        return [self.puuid, self.platform, self.activity]


class Handler:
    accounts = None
    is_shutdown = False

    def __init__(self, configs):
        self.logging = logging.getLogger("Handler")
        self.service = configs.services.summoner_tracker
        self.config = configs
        self.endpoint_url = (
            f"{configs.connections.proxy.protocol}://%s.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/%s"
        )
        self.proxy = "%s://%s" % (configs.connections.proxy.protocol, configs.connections.proxy.location)

    async def init(self):
        psq_con = self.config.connections.postgres
        self.postgres = await asyncpg.create_pool(
            host=psq_con.hostname,
            port=psq_con.port,
            user=psq_con.user,
            database=psq_con.database,
            password=os.getenv(psq_con.password_env),
        )

    async def init_shutdown(self, *args, **kwargs):
        """Shutdown handler"""
        self.logging.info("Received shutdown signal.")
        self.is_shutdown = True

    async def handle_shutdown(self):
        """Close db connection pool after services have shut down."""
        await self.postgres.close()

    async def run(self):
        """Run."""
        await self.init()
        while not self.is_shutdown:
            async with self.postgres.acquire() as connection:
                async with connection.transaction():
                    self.accounts = {}
                    tasks = await connection.fetch("""
                        SELECT puuid, platform, last_activity
                        FROM summoner
                        WHERE last_updated IS NULL
                         OR last_updated + INTERVAL '%s days' <= CURRENT_DATE
                        ORDER BY last_updated
                        LIMIT 10
                        FOR UPDATE 
                        SKIP LOCKED 
                    """ % self.service.update_interval)
                    if not tasks:
                        await asyncio.sleep(5)
                        continue
                    for task in tasks:
                        self.accounts[task['puuid']] = Account(task)
                    async with aiohttp.ClientSession() as session:
                        await asyncio.gather(*[
                            asyncio.create_task(self.worker(session, platform)) for platform in
                            self.config.statics.enums.platforms
                        ])
                    if self.is_shutdown:
                        break
                    updated = []
                    unchanged = []
                    for account in self.accounts.values():
                        if account.updated:
                            updated.append(account.result())
                        else:
                            unchanged.append(account.puuid)
                    # Updated summoners
                    if updated:
                        prep = await connection.prepare(
                            """UPDATE summoner
                               SET  platform = $2,
                                    last_activity = $3,
                                    last_updated = CURRENT_DATE 
                                WHERE puuid = $1
                                """
                        )
                        await prep.executemany(updated)
                    # Unchanged summoners
                    if unchanged:
                        await connection.execute(
                            """ UPDATE summoner
                                SET last_updated = CURRENT_DATE
                                WHERE puuid = ANY($1::varchar)
                            """, unchanged
                        )
                    self.logging.info("Found %s updated and %s unupdated users.",
                                      len(updated), len(unchanged))

    async def worker(self, session, platform):
        """Make calls for a user on a specific server."""
        for puuid, account in self.accounts.items():
            url = self.endpoint_url % (platform.lower(), puuid)
            tries = 0
            while not self.is_shutdown:
                try:
                    async with session.get(url, proxy=self.proxy) as response:
                        if response.status == 404:
                            break
                        if response.status == 200:
                            data = await response.json()
                            account.add_result(platform, data['revisionDate'])
                            break
                        if response.status == 429:
                            await asyncio.sleep(0.5)
                        if response.status == 430:
                            data = await response.json()
                            wait_until = datetime.fromtimestamp(data["Retry-At"])
                            seconds = (wait_until - datetime.now()).total_seconds()
                            seconds = max(0.1, seconds)
                            await asyncio.sleep(seconds)

                except aiohttp.ContentTypeError:
                    continue
                except asyncio.CancelledError:
                    continue
                tries += 1
                if tries >= 5:
                    break
