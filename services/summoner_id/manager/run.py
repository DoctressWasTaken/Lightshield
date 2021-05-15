import asyncio
import logging
import os
import signal
from datetime import datetime, timedelta
import settings
import uvloop
from connection_manager.buffer import RedisConnector
from connection_manager.persistent import PostgresConnector

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
        self.redis = RedisConnector()
        self.db = PostgresConnector(user=settings.SERVER)

    async def init(self):
        async with self.redis.get_connection() as connection:
            await connection.delete("%s_summoner_id_in_progress" % settings.SERVER)
            await connection.delete("%s_summoner_id_tasks" % settings.SERVER)

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
