import asyncio
import json
import logging
import os
import signal

import aioredis
import asyncpg
import uvloop

from lightshield.proxy import Proxy
from service import Service

uvloop.install()

if "DEBUG" in os.environ:
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
else:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
logging.debug("Debug enabled.")

services = [
    "EUW1",
    "EUN1",
    "TR1",
    "RU",
    "NA1",
    "BR1",
    "LA1",
    "LA2",
    "OC1",
    "KR",
    "JP1",
]


class Handler:
    is_shutdown = False
    platforms = {}
    redis = postgres = None

    def __init__(self):
        self.logging = logging.getLogger("Handler")
        self.api_key = None
        self.proxy = Proxy()

    async def init(self):
        self.redis = aioredis.from_url(
            "redis://redis:6379", encoding="utf-8", decode_responses=True
        )
        self.postgres = await asyncpg.create_pool(
            host="postgres", port=5432, user="postgres", database="lightshield"
        )
        await self.proxy.init("redis", 6379)

        for platform in services:
            s = Service(platform, self)
            await s.init()
            self.platforms[platform] = s
        self.logging.info("Ready.")

    async def shutdown(self, *args, **kwargs):
        """Initiate shutdown."""
        self.is_shutdown = True

    async def check_active(self):
        """Confirm that the service is supposed to run."""
        try:
            status = await self.redis.get("service_league_ranking")
            return status == "true"
        except Exception as err:
            print("Check Exception", err)
            return False

    async def get_apiKey(self):
        """Pull API Key from Redis."""
        self.api_key = await self.redis.get("apiKey")

    async def check_platforms(self):
        """Platform check."""
        region_status = {}
        try:
            data = json.loads(await self.redis.get("regions"))
            for region, region_data in data.items():
                overwrite = None
                if not region_data["status"]:
                    overwrite = True
                for platform, status in region_data["platforms"].items():
                    region_status[platform] = False if overwrite else status
        finally:
            return region_status

    async def run(self):
        """Run."""
        await self.init()
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda signame=sig: asyncio.create_task(self.shutdown())
            )

        while not self.is_shutdown:
            try:
                await self.get_apiKey()
                if not self.api_key.startswith("RGAPI"):
                    for platform in self.platforms.values():
                        await platform.stop()
                    continue
                if not await self.check_active():
                    for platform in self.platforms.values():
                        await platform.stop()
                    continue
                platforms = await self.check_platforms()
                for platform, key in platforms.items():
                    if key:
                        await self.platforms[platform].start()
                        continue
                    await self.platforms[platform].stop()
            except Exception as err:
                print("Run Exception", err)
            finally:
                await asyncio.sleep(5)


if __name__ == "__main__":
    handler = Handler()
    asyncio.run(handler.run())
