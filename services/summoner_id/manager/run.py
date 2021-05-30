import aioredis
import asyncio
import asyncpg
import logging
import os
import signal
import uvloop
from datetime import datetime, timedelta
from lightshield import settings

# uvloop.install()
if "DEBUG" in os.environ:
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)8s %(name)s %(message)s"
    )
else:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)8s %(name)s %(message)s"
    )

uvloop.install()


class Manager:
    stopped = False

    def __init__(self):
        self.logging = logging.getLogger("Main")
        level = logging.INFO
        if settings.DEBUG:
            level = logging.DEBUG
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        self.logging.addHandler(handler)
        # Buffer
        self.redis = None
        # Postgres
        self.db = None

    async def init(self):
        self.db = await asyncpg.create_pool(
            host=settings.PERSISTENT_HOST,
            port=settings.PERSISTENT_PORT,
            user=settings.PERSISTENT_USER,
            database=settings.PERSISTENT_DATABASE,
        )

        self.redis = await aioredis.create_redis_pool(
            (settings.BUFFER_HOST, settings.BUFFER_PORT), encoding="utf-8"
        )
        await self.redis.delete("%s_summoner_id_in_progress" % settings.SERVER)
        await self.redis.delete("%s_summoner_id_tasks" % settings.SERVER)

    def shutdown(self):
        self.stopped = True

    async def run(self):
        await self.init()
        minimum = 100  # Local minimum that gets reset every time tasks are inserted
        blocked = False
        while not self.stopped:
            # Drop timed out tasks
            limit = int(
                (
                    datetime.utcnow() - timedelta(minutes=settings.RESERVE_MINUTES)
                ).timestamp()
            )
            async with self.redis.get_connection() as buffer:
                await buffer.zremrangebyscore(
                    "%s_summoner_id_in_progress" % settings.SERVER, max=limit
                )
                # Check remaining buffer size
                if (
                    size := await buffer.scard("%s_summoner_id_tasks" % settings.SERVER)
                ) >= 1000:
                    await asyncio.sleep(10)
                    continue
            async with self.db.get_connection() as db:
                result = await db.fetch(
                    """
                    SELECT summoner_id
                    FROM %s.summoner
                    WHERE account_id IS NULL
                    LIMIT 2000;
                    """
                    % settings.SERVER
                )
            if len(result) - size < minimum:
                if not blocked:
                    self.logging.info("No tasks found.")
                blocked = True
                minimum -= 1
                await asyncio.sleep(30)
                continue
            minimum = 100
            self.logging.info("%s tasks remaining.", size)
            async with self.redis.get_connection() as buffer:
                for entry in result:
                    if await buffer.sismember(
                        "%s_summoner_id_tasks" % settings.SERVER, entry["summoner_id"]
                    ):
                        continue
                    await buffer.sadd(
                        "%s_summoner_id_tasks" % settings.SERVER, entry["summoner_id"]
                    )
                    if (
                        await buffer.scard("%s_summoner_id_tasks" % settings.SERVER)
                        >= 2000
                    ):
                        break
                self.logging.info(
                    "Filled tasks to %s.",
                    await buffer.scard("%s_summoner_id_tasks" % settings.SERVER),
                )
            await asyncio.sleep(5)
        await self.redis.close()
        await self.db.close()


async def main():
    manager = Manager()

    def shutdown_handler():
        """Shutdown."""
        manager.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    await manager.run()


if __name__ == "__main__":
    asyncio.run(main())
