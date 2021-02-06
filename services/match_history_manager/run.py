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
        await self.redis.delete('match_history_in_progress')
        await self.redis.delete('match_history_tasks')

    def shutdown(self):
        self.stopped = True

    async def get_tasks(self):
        """Return tasks and full_refresh flag.

        If there are non-initialized user found only those will be selected.
        If none are found a list of the user with the most new games are returned.
        """
        try:
            conn = asyncpg.connect("postgresql://postgres@postgres/raw")
            if result := await conn.fetch('''
                SELECT account_id, 
                       wins, 
                       losses
                FROM summoner
                WHERE wins_last_updated IS NULL 
                AND account_id IS NOT NULL
                LIMIT 2000;
                '''):
                return result, True
            return await conn.fetch('''
                SELECT account_id, 
                       wins, 
                       losses, 
                       wins_last_updated, 
                       losses_last_updated
                FROM summoner
                WHERE wins_last_updated IS NOT NULL
                AND account_id IS NOT NULL
                ORDER BY (wins + losses - wins_last_updated - losses_last_updated) DESC
                LIMIT 2000;
                '''), False
        finally:
            await conn.close()

    async def run(self):
        await self.init()

        while not self.stopped:
            # Drop timed out tasks
            limit = int((datetime.utcnow() - timedelta(minutes=10)).timestamp())
            await self.redis.zremrangebyscore('match_history_in_progress', max=limit)
            # Check remaining buffer size
            if (size := await self.redis.zcard('match_history_tasks')) < 1000:
                self.logging.info("%s tasks remaining.", size)
                # Pull new tasks
                result, full_refreshes = await self.get_tasks()
                if not result:
                    self.logging.info("No tasks found.")
                    await asyncio.sleep(60)
                    continue
                # Add new tasks
                for entry in result:
                    # Each entry will always be refered to by account_id
                    if await self.redis.zscore('match_history_in_progress', entry['account_id']):
                        continue
                    if not full_refreshes:
                        z_index = 9999
                    else:
                        z_index = entry['wins'] + entry['losses'] - entry['wins_last_updated'] - entry[
                            'losses_last_updated']

                    package = {key: entry[key] for key in
                               ['wins', 'losses', 'wins_last_updated', 'losses_last_updated']}

                    # Insert task hook
                    await self.redis.zadd('match_history_tasks', z_index, entry['account_id'])
                    # Insert task hash
                    await self.redis.hmset_dict(
                        "%s:%s" % (entry['account_id'], z_index), package
                    )
                self.logging.info("Filled tasks to %s.", await self.redis.scard('match_history_tasks'))

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
