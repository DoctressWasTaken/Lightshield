import asyncio
import json
import logging
import os
import signal
import tracemalloc

import aioredis
import asyncpg
import uvloop

tracemalloc.start()

uvloop.install()

from service import Platform
from lightshield.proxy import Proxy

if "DEBUG" in os.environ:
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
else:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
logging.debug("Debug enabled")

regions = ["europe", "americas", "asia"]

server = {
    "europe": ["EUW", "EUNE", "TR", "RU"],
    "americas": ["NA", "BR", "LAN", "LAS", "OCE"],
    "asia": ["KR", "JP"],
}


class Handler:
    api_key = ""
    _runner = None

    def __init__(self):
        self.logging = logging.getLogger("Service")
        # Buffer
        self.postgres = None
        self.redis = None
        self.platforms = {}
        self.proxy = Proxy()

    async def init(self):
        self.redis = aioredis.from_url(
            "redis://redis:6379", encoding="utf-8", decode_responses=True
        )
        await self.proxy.init("redis", 6379)
        self.postgres = await asyncpg.create_pool(
            host="postgres",
            port=5432,
            user="postgres",
            database="lightshield",
        )
        for region, platforms in server.items():
            p = Platform(region, platforms, self)
            await p.init()
            self.platforms[region] = p

    async def shutdown(self):
        """Shutdown handler"""
        self._runner.cancel()
        await asyncio.gather(
            *[
                asyncio.create_task(platform.shutdown())
                for platform in self.platforms.values()
            ]
        )
        self.redis.close()
        await self.redis.wait_closed()
        await self.postgres.close()
        return

    async def check_active(self):
        """Confirm that the service is supposed to run."""
        try:
            status = await self.redis.get("service_match_timeline")
            return status == "true"
        except Exception as err:
            print("Check Exception", err)
            return False

    async def get_apiKey(self):
        """Pull API Key from Redis."""
        self.api_key = await self.redis.get("apiKey")
        if not self.api_key.startswith("RGAPI"):
            return False
        return True

    async def check_platforms(self):
        """Platform check."""
        region_status = {}
        try:
            data = json.loads(await self.redis.get("regions"))
            for region, region_data in data.items():
                region_status[region.lower()] = region_data["status"]
        finally:
            return region_status

    async def test(self):
        while True:
            # self.logging.info(self.h.heap())
            await asyncio.sleep(15)

    async def runner(self):
        """Main application loop"""
        asyncio.create_task(self.test())
        while True:
            await self.get_apiKey()
            if not self.api_key.startswith("RGAPI") or not await self.check_active():
                await asyncio.gather(
                    *[
                        asyncio.create_task(platform.stop())
                        for platform in self.platforms.values()
                    ]
                )
                continue
            platform_status = await self.check_platforms()
            await asyncio.gather(
                *[
                    asyncio.create_task(
                        self.platforms[platform].start()
                        if active
                        else self.platforms[platform].stop()
                    )
                    for platform, active in platform_status.items()
                ]
            )

    async def run(self):
        """Run."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda signame=sig: asyncio.create_task(self.shutdown())
            )
        await self.init()
        self._runner = asyncio.create_task(self.runner())
        try:
            await self._runner
        except asyncio.CancelledError:
            return


if __name__ == "__main__":
    manager = Handler()
    asyncio.run(manager.run())
