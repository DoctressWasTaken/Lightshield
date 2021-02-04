import asyncio
import signal
from datetime import datetime, timedelta

import aioredis
import asyncpg
import uvloop

uvloop.install()


class Manager:
    stopped = False

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
            if await self.redis.scard('tasks') < 250:
                # Pull new tasks
                conn = await asyncpg.connect("postgresql://postgres@postgres/raw")
                result = await conn.fetch('''
                    SELECT summoner_id
                    FROM summoner
                    WHERE account_id IS NULL
                    LIMIT 500;
                    ''')
                await conn.close()
                # Add new tasks
                for entry in result:

                    await self.redis.sadd('task', entry['summoner_id'])
                    if self.redis.scard('tasks') >= 500:
                        break

            await asyncio.sleep(10)
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
