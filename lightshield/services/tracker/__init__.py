"""Tracking Module.

Transfers tracking data from redis into the selected db.

"""
import asyncio
from lightshield.config import Config
import aioredis
import os
from datetime import datetime


class Handler:
    is_shutdown = False

    def __init__(self):
        self.config = Config()
        self.connection = self.config.get_db_connection()

    async def init_shutdown(self):
        self.is_shutdown = True

    async def run(self):
        host = os.getenv("REDIS_HOST", "localhost")
        port = os.getenv("REDIS_PORT", 6379)
        redis = aioredis.from_url(
            "redis://%s:%s" % (host, port), encoding="utf-8", decode_responses=True
        )
        last_timestamp = 0
        db = await self.connection.init()
        while not self.is_shutdown:
            tasks = []
            current = int(datetime.now().timestamp())
            keys = await redis.keys("tracking:*")
            splits = [key.split(":")[1:] + [key] for key in keys]
            for timestamp, env, api, platform, endpoint, key in splits:
                timestamp = int(timestamp)
                if timestamp <= last_timestamp or timestamp >= current:
                    continue
                for key, val in (await redis.hgetall(key)).items():
                    tasks.append(
                        [
                            int(key),
                            int(val),
                            datetime.fromtimestamp(timestamp),
                            platform.upper(),
                            endpoint,
                            api,
                        ]
                    )
            if tasks:
                async with db.acquire() as connection:
                    query = """INSERT INTO "tracking"."requests" 
                        (response_type, request_count, interval_time, platform, endpoint, api_key)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (response_type, interval_time, platform, endpoint, api_key)
                        DO UPDATE SET request_count = EXCLUDED.request_count
                    """
                    await connection.executemany(query, tasks)
            last_timestamp = current - 1
            for i in range(5):
                if self.is_shutdown:
                    break
                await asyncio.sleep(2)
