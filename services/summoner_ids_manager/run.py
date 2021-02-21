import asyncio
import logging
import os
import signal
from datetime import datetime, timedelta

import aioredis
import asyncpg
import uvloop

uvloop.install()


class Manager:
    stopped = False

    def __init__(self):
        self.logging = logging.getLogger("Main")
        level = logging.INFO
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter('%(asctime)s %(message)s'))
        self.logging.addHandler(handler)
        self.server = os.environ['SERVER']
        self.db_host = os.environ['DB_HOST']

    async def init(self):
        self.redis = await aioredis.create_redis(
            ('redis', 6379))
        await self.redis.delete('%s_summoner_id_in_progress' % self.server)
        await self.redis.delete('%s_summoner_id_tasks' % self.server)

    def shutdown(self):
        self.stopped = True

    async def run(self):
        await self.init()
        min_threshold = 100
        while not self.stopped:
            # Drop timed out tasks
            limit = int((datetime.utcnow() - timedelta(minutes=10)).timestamp())
            await self.redis.zremrangebyscore('%s_summoner_id_in_progress' % self.server, max=limit)
            # Check remaining buffer size
            if (size := await self.redis.scard('%s_summoner_id_tasks' % self.server)) < 1000:
                self.logging.info("%s tasks remaining.", size)
                # Pull new tasks
                conn = await asyncpg.connect(
                    "postgresql://%s@%s/%s" % (self.server.lower(), self.db_host, self.server.lower()))
                result = await conn.fetch('''
                    SELECT summoner_id
                    FROM summoner
                    WHERE account_id IS NULL
                    LIMIT 2000;
                    ''')
                await conn.close()
                if len(result) < min_threshold:
                    self.logging.info("Not enough tasks found.")
                    await asyncio.sleep(60)
                    min_threshold = max(1, min_threshold - 1)
                    continue
                # Add new tasks
                for entry in result:
                    if await self.redis.sismember('%s_summoner_id_tasks' % self.server, entry['summoner_id']):
                        continue
                    await self.redis.sadd('%s_summoner_id_tasks' % self.server, entry['summoner_id'])
                    if await self.redis.scard('%s_summoner_id_tasks' % self.server) >= 2000:
                        break
                self.logging.info("Filled tasks to %s.", await self.redis.scard('%s_summoner_id_tasks' % self.server))

            await asyncio.sleep(5)
        await self.redis.close()


async def main():
    manager = Manager()

    def shutdown_handler():
        """Shutdown."""
        manager.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    await manager.run()


if __name__ == "__main__":
    asyncio.run(main())
