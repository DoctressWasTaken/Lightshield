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

    def shutdown(self):
        self.stopped = True

    async def run(self):
        await self.init()

        while not self.stopped:
            # Drop timed out tasks
            limit = (datetime.utcnow() - timedelta(minutes=10)).timestamp()
            await self.redis.zremrangebyscore('in_progress', max=limit)
            # Check remaining buffer size
            if (size := await self.redis.scard('tasks')) < 1000:
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
                # Add new tasks
                for entry in result:
                    self.logging.info(entry['summoner_id'])
                    await self.redis.sadd('task', entry['summoner_id'])
                    if await self.redis.scard('tasks') >= 2000:
                        break
                self.logging.info("Filled tasks to %s.", await self.redis.scard('tasks'))

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
