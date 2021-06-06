import asyncio
import logging
import os
import signal
import traceback
from datetime import datetime, timedelta
import uvloop
from lightshield import settings
import aioredis
import asyncpg

uvloop.install()
if "DEBUG" in os.environ:
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
else:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )


class Manager:
    stopped = False

    def __init__(self):
        self.logging = logging.getLogger("Service")
        # Buffer
        self.redis = None
        # Postgres
        self.db = None

    async def init(self):
        self.db = await asyncpg.create_pool(
            host=settings.PERSISTENT_HOST,
            port=settings.PERSISTENT_PORT,
            user=settings.SERVER,
            password=settings.PERSISTENT_PASSWORD,
            database=settings.PERSISTENT_DATABASE,
        )

        self.redis = await aioredis.create_redis_pool(
            (settings.REDIS_HOST, settings.REDIS_PORT), encoding="utf-8"
        )
        await self.redis.delete("%s_match_details_in_progress" % settings.SERVER)
        await self.redis.delete("%s_match_details_tasks" % settings.SERVER)

    def shutdown(self):
        self.stopped = True

    async def get_tasks(self):
        """Return tasks and full_refresh flag.

        If there are non-initialized user found only those will be selected.
        If none are found a list of the user with the most new games are returned.
        """
        async with self.db.acquire() as connection:
            tasks = await connection.fetch(
                """
                SELECT match_id, queue, timestamp
                FROM %s.match
                WHERE details_pulled IS NULL
                AND (timestamp::date >= %s OR timestamp IS NULL) 
                LIMIT $1;
                """
                % (settings.SERVER, settings.MAX_AGE),
                settings.QUEUE_LIMIT * 2,
            )
            return tasks

    async def run(self):
        await self.init()
        min_count = 100
        blocked = False
        try:
            while not self.stopped:
                # Drop timed out tasks
                limit = int(
                    (
                        datetime.utcnow() - timedelta(minutes=settings.RESERVE_MINUTES)
                    ).timestamp()
                )
                await self.redis.zremrangebyscore(
                    "%s_match_details_in_progress" % settings.SERVER, max=limit
                )
                # Check remaining buffer size
                if (
                    size := await self.redis.scard(
                        "%s_match_details_tasks" % settings.SERVER
                    )
                ) >= settings.QUEUE_LIMIT:
                    await asyncio.sleep(10)
                    continue
                # Pull new tasks
                result = await self.get_tasks()
                if len(result) - size < min_count:
                    if not blocked:
                        self.logging.info("%s tasks remaining.", size)
                        self.logging.info("No tasks found.")
                        blocked = True
                    min_count -= 1
                    await asyncio.sleep(30)
                    continue
                min_count = 100
                self.logging.info("%s tasks remaining.", size)
                self.logging.info("Found %s tasks.", len(result))
                # Add new tasks
                for entry in result:
                    # Each entry will always be refered to by account_id
                    if await self.redis.zscore(
                        "%s_match_details_in_progress" % settings.SERVER,
                        entry["match_id"],
                    ):
                        continue
                    # Insert task hook
                    await self.redis.sadd(
                        "%s_match_details_tasks" % settings.SERVER,
                        entry["match_id"],
                    )

                self.logging.info(
                    "Filled tasks to %s.",
                    await self.redis.scard("%s_match_details_tasks" % settings.SERVER),
                )
                await asyncio.sleep(1)

                await asyncio.sleep(5)

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
