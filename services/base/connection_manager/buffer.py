import os
from contextlib import asynccontextmanager
import asyncio
import aioredis
import settings


class RedisConnector:
    def __init__(self):
        self.host = settings.BUFFER_HOST
        self.port = settings.BUFFER_PORT
        self.lock = None

        self.connection = None
        self.connection: aioredis.ConnectionsPool

    async def create_lock(self):
        self.lock = asyncio.Lock()

    @asynccontextmanager
    async def get_connection(self, exclusive=False):
        if exclusive:
            await self.lock.acquire()
        if not self.connection or self.connection.closed:
            self.connection = await aioredis.create_redis_pool(
                (self.host, self.port), encoding="utf-8"
            )

        try:
            yield self.connection
        finally:
            if exclusive:
                self.lock.release()

    async def close(self):
        await self.connection.close()
