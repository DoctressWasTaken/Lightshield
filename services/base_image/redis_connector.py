
import asyncio
import aioredis
import logging
from aioredis.errors import ReplyError

class Redis:

    def __init__(self, host='redis', port=6379):
        """Initiate the redis connection handler.

        Optional arguments host and port. Default to redis:6379.
        """
        self.host = host
        self.port = port
        self.logging = logging.getLogger("redis")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [REDIS] %(message)s'))
        self.logging.addHandler(ch)
        self.redis = None

    async def connect(self):
        time = 0.5
        while not self.redis or self.redis.closed:
            try:
                self.redis = await aioredis.create_redis_pool(
                    (self.host, self.port), db=0, encoding='utf-8')
            except ReplyError:
                await asyncio.sleep(5)
                continue
            await asyncio.sleep(time)
            time = min(time + 0.5, 5)
            if time == 5:
                self.logging.info("Connection to redis could not be established")

    async def hgetall(self, key):
        await self.connect()
        return await self.redis.hgetall(key)

    async def hset(self, key, mapping):
        await self.connect()
        await self.redis.hmset_dict(key, mapping)
        return

    async def sismember(self, _set, key):
        await self.connect()
        await self.redis.sismember(_set, str(key))


    async def sadd(self, _set, key):
        await self.connect()
        await self.redis.sadd(_set, str(key))
