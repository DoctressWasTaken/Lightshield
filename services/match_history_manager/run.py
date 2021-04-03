import asyncio
import logging
import os
import signal
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
        self.min_matches = int(os.environ["MIN_MATCHES"])
        self.server = os.environ["SERVER"]
        self.logging.addHandler(handler)

        self.redis = RedisConnector()
        self.db = PostgresConnector(user=self.server.lower())

    async def init(self):
        async with self.redis.get_connection() as buffer:
            await buffer.delete("%s_match_history_in_progress" % self.server)
            await buffer.delete("%s_match_history_tasks" % self.server)

    def shutdown(self):
        self.stopped = True

    async def get_tasks(self):
        """Return tasks and full_refresh flag.

        If there are non-initialized user found only those will be selected.
        If none are found a list of the user with the most new games are returned.
        """
        async with self.db.get_connection() as db:
            full_refresh = await db.fetch(
                """
                                    SELECT account_id, 
                                           wins, 
                                           losses
                                    FROM %s.summoner
                                    WHERE wins_last_updated IS NULL 
                                    AND account_id IS NOT NULL
                                    ORDER BY (wins + losses) DESC
                                    LIMIT 2000;
                                    """
                % self.server.lower()
            )
            if len(full_refresh) >= 100:
                self.logging.info("Found %s full refresh tasks." % len(full_refresh))
                return full_refresh, True
            partial_refresh = await db.fetch(
                """
            SELECT account_id, 
                   wins, 
                   losses, 
                   wins_last_updated, 
                   losses_last_updated
            FROM %s.summoner
            WHERE wins_last_updated IS NOT NULL
            AND account_id IS NOT NULL
            AND (wins + losses - wins_last_updated - losses_last_updated) >= $1
            ORDER BY (wins + losses - wins_last_updated - losses_last_updated) DESC
            LIMIT 2000;
            """
                % self.server.lower(),
                self.min_matches,
            )
            self.logging.info("Found %s partial refresh tasks." % len(partial_refresh))
            return partial_refresh, False

    async def run(self):
        await self.init()

        while not self.stopped:
            # Drop timed out tasks
            limit = int((datetime.utcnow() - timedelta(minutes=10)).timestamp())
            async with self.redis.get_connection() as buffer:
                await buffer.zremrangebyscore(
                    "%s_match_history_in_progress" % self.server, max=limit
                )
                # Check remaining buffer size
                if (
                    size := await buffer.zcard("%s_match_history_tasks" % self.server)
                ) >= 1000:
                    await asyncio.sleep(10)
                    continue

                result, full_refreshes = await self.get_tasks()
                if not result:
                    self.logging.info("No tasks found.")
                    await asyncio.sleep(60)
                    continue
                # Add new tasks
                self.logging.info("%s tasks remaining.", size)
                for entry in result:
                    # Each entry will always be refered to by account_id
                    if await buffer.zscore(
                        "%s_match_history_in_progress" % self.server,
                        entry["account_id"],
                    ):
                        continue
                    if full_refreshes:
                        z_index = 9999
                        package = {key: entry[key] for key in ["wins", "losses"]}
                    else:
                        z_index = (
                            entry["wins"]
                            + entry["losses"]
                            - entry["wins_last_updated"]
                            - entry["losses_last_updated"]
                        )
                        package = {
                            key: entry[key]
                            for key in [
                                "wins",
                                "losses",
                                "wins_last_updated",
                                "losses_last_updated",
                            ]
                        }

                    # Insert task hook
                    await buffer.zadd(
                        "%s_match_history_tasks" % self.server,
                        z_index,
                        entry["account_id"],
                    )
                    # Insert task hash
                    await buffer.hmset_dict(
                        "%s:%s:%s" % (self.server, entry["account_id"], z_index),
                        package,
                    )
                self.logging.info(
                    "Filled tasks to %s.",
                    await buffer.zcard("%s_match_history_tasks" % self.server),
                )

        await asyncio.sleep(5)


async def main():
    manager = Manager()

    def shutdown_handler():
        """Shutdown."""
        manager.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    await manager.run()


if __name__ == "__main__":
    asyncio.run(main())
