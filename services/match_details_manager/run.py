import asyncio
import logging
import os
import signal
import traceback
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
        self.limit = int(os.environ['LIMIT'])

    async def init(self):
        self.redis = await aioredis.create_redis(
            ('redis', 6379))
        await self.redis.delete('match_details_in_progress')
        await self.redis.delete('match_details_tasks')

    def shutdown(self):
        self.stopped = True

    async def get_tasks(self):
        """Return tasks and full_refresh flag.

        If there are non-initialized user found only those will be selected.
        If none are found a list of the user with the most new games are returned.
        """
        conn = await asyncpg.connect("postgresql://postgres@postgres/raw")
        try:
            return await conn.fetch('''
                SELECT match_id
                FROM match
                WHERE details_pulled IS NULL
                AND DATE(timestamp) >= '2021-01-01' 
                ORDER BY timestamp DESC
                LIMIT $1;
                ''', self.limit * 2)
        finally:
            await conn.close()

    async def run(self):
        await self.init()
        try:
            while not self.stopped:
                # Drop timed out tasks
                limit = int((datetime.utcnow() - timedelta(minutes=10)).timestamp())
                await self.redis.zremrangebyscore('match_details_in_progress', max=limit)
                # Check remaining buffer size
                if (size := await self.redis.scard('match_details_tasks')) < self.limit:
                    self.logging.info("%s tasks remaining.", size)
                    # Pull new tasks
                    result = await self.get_tasks()
                    if not result:
                        self.logging.info("No tasks found.")
                        await asyncio.sleep(60)
                        continue
                    # Add new tasks
                    for entry in result:
                        # Each entry will always be refered to by account_id
                        if await self.redis.zscore('match_details_in_progress', entry['match_id']):
                            continue
                        # Insert task hook
                        await self.redis.sadd('match_details_tasks', entry['match_id'])

                    self.logging.info("Filled tasks to %s.", await self.redis.scard('match_details_tasks'))
                    await asyncio.sleep(1)
                    continue
                await asyncio.sleep(5)

            await self.redis.close()

        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)


async def main():
    manager = Manager()

    def shutdown_handler():
        """Shutdown."""
        manager.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    await manager.run()


if __name__ == "__main__":
    asyncio.run(main())
