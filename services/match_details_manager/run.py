import asyncio
import logging
import os
import signal
import traceback
from datetime import datetime, timedelta

import uvloop

uvloop.install()

from connection_manager.buffer import RedisConnector
from connection_manager.persistent import PostgresConnector


class Manager:
    stopped = False

    def __init__(self):
        self.logging = logging.getLogger("Main")
        level = logging.INFO
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        self.logging.addHandler(handler)
        self.limit = int(os.environ["LIMIT"])
        self.server = os.environ["SERVER"]
        self.block_limit = int(os.environ['TASK_BLOCKING'])
        self.details_cutoff = os.environ['DETAILS_CUTOFF']
        self.redis = RedisConnector()
        self.db = PostgresConnector(user=self.server.lower())

    async def init(self):
        async with self.redis.get_connection() as buffer:
            await buffer.delete("%s_match_details_in_progress" % self.server)
            await buffer.delete("%s_match_details_tasks" % self.server)

    def shutdown(self):
        self.stopped = True

    async def get_tasks(self):
        """Return tasks and full_refresh flag.

        If there are non-initialized user found only those will be selected.
        If none are found a list of the user with the most new games are returned.
        """
        async with self.db.get_connection() as db:
            tasks = await db.fetch(
                """
                SELECT match_id
                FROM %s.match
                WHERE details_pulled IS NULL
                AND DATE(timestamp) >= %s
                ORDER BY timestamp DESC
                LIMIT $1;
                """
                % (self.server.lower(), self.details_cutoff),
                self.limit * 2,
            )
            self.logging.info("Found %s tasks." % len(tasks))
            return tasks

    async def run(self):
        await self.init()
        min_count = 100
        blocked = False
        try:
            while not self.stopped:
                # Drop timed out tasks
                limit = int((datetime.utcnow() - timedelta(minutes=self.block_limit)).timestamp())
                async with self.redis.get_connection() as buffer:
                    await buffer.zremrangebyscore(
                        "%s_match_details_in_progress" % self.server, max=limit
                    )
                    # Check remaining buffer size
                    if (
                            size := await buffer.scard(
                                "%s_match_details_tasks" % self.server
                            )
                    ) >= self.limit:
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
                        if await buffer.zscore(
                                "%s_match_details_in_progress" % self.server,
                                entry["match_id"],
                        ):
                            continue
                        # Insert task hook
                        await buffer.sadd(
                            "%s_match_details_tasks" % self.server, entry["match_id"]
                        )

                    self.logging.info(
                        "Filled tasks to %s.",
                        await buffer.scard("%s_match_details_tasks" % self.server),
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
