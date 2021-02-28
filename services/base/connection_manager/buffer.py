import os
from contextlib import asynccontextmanager

import aioredis


class RedisConnector:

    def __init__(self):
        self.host = os.environ['BUFFER_HOST']
        self.port = int(os.environ['BUFFER_PORT'])

        self.connection = None
        self.connection: aioredis.ConnectionsPool

    @asynccontextmanager
    async def get_connection(self):
        if not self.connection or self.connection.closed:
            self.connection = await aioredis.create_redis_pool((self.host, self.port), encoding="utf-8")

        try:
            yield self.connection
        finally:
            pass

    async def close(self):
        await self.connection.close()
