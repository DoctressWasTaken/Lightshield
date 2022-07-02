"""Match History"""
import asyncio
import json
import logging
import os
import signal

import aioredis
import asyncpg
import uvloop

uvloop.install()

from service import Platform
from lightshield.proxy import Proxy

class Handler:

    def __init__(self, configs):
        self.logging = logging.getLogger("Service")
        self.connections = configs.get("connections")
        self.config = configs.get("services")['match_history']
        self.mapping = configs.get("statics")['mapping']
        # Buffer
        self.postgres = None
        self.redis = None
        self.platforms = {}
        self.proxy = Proxy()

    async def init(self):
        psq_con = self.connections.get("postgres")
        self.postgres = await asyncpg.create_pool(
            host=psq_con.get("hostname"),
            port=psq_con.get("port"),
            user=psq_con.get("user"),
            database=psq_con.get("database"),
            password=os.getenv(psq_con.get("password_env")),
        )
        self.redis = aioredis.from_url(
            "redis://redis:6379", encoding="utf-8", decode_responses=True
        )
        await self.proxy.init("redis", 6379)

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
            status = await self.redis.get("service_match_history")
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
                region_status[region.lower()] = region_data["status"]
        finally:
            return region_status

    async def runner(self):
        """Main application loop"""
        while True:
            await self.get_apiKey()
            if not self.api_key.startswith("RGAPI"):
                for platform in self.platforms.values():
                    await platform.stop()
                continue
            if not await self.check_active():
                for platform in self.platforms.values():
                    await platform.stop()
                continue
            platform_status = await self.check_platforms()
            for platform, active in platform_status.items():
                if active:
                    await self.platforms[platform].start()
                    continue
                await self.platforms[platform].stop()
            await asyncio.sleep(5)

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
