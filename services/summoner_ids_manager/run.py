import asyncio
import logging
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

    async def init(self):
        self.redis = await aioredis.create_redis(
            ('redis', 6379))
        await self.redis.delete('summoner_id_in_progress')
        await self.redis.delete('summoner_id_tasks')
        try:
            await self.redis.delete('tasks')
            await self.redis.delete('in_progress')
        except:
            pass

    def shutdown(self):
        self.stopped = True

    async def run(self):
        await self.init()
        min_threshold = 100
        while not self.stopped:
            # Drop timed out tasks
            limit = int((datetime.utcnow() - timedelta(minutes=10)).timestamp())
            await self.redis.zremrangebyscore('summoner_id_in_progress', max=limit)
            # Check remaining buffer size
            if (size := await self.redis.scard('summoner_id_tasks')) < 1000:
                self.logging.info("%s tasks remaining.", size)
                # Pull new tasks
                conn = await asyncpg.connect("postgresql://postgres@postgres/raw")
                result = await conn.fetch('''
                    SELECT summoner_id
                    FROM summoner
                    WHERE account_id IS NULL
                    LIMIT 2000;
                    ''')
                await conn.close()
                if len(result < min_threshold):
                    self.logging.info("Not enough tasks found.")
                    await asyncio.sleep(60)
                    min_threshold = max(1, min_threshold - 1)
                    continue
                # Add new tasks
                for entry in result:
                    if await self.redis.sismember('summoner_id_tasks', entry['summoner_id']):
                        continue
                    await self.redis.sadd('summoner_id_tasks', entry['summoner_id'])
                    if await self.redis.scard('summoner_id_tasks') >= 2000:
                        break
                self.logging.info("Filled tasks to %s.", await self.redis.scard('summoner_id_tasks'))

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
