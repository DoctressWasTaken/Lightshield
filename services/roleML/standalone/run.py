import asyncio
import json
import logging
import os
import signal
import traceback
import settings
import roleml
from connection_manager.persistent import PostgresConnector
from roleml.exceptions import IncorrectMap, MatchTooShort


class Manager:
    stopped = False

    def __init__(self, queues):
        self.logging = logging.getLogger("Main")
        level = logging.INFO
        if settings.DEBUG:
            level = logging.DEBUG
        self.logging.setLevel(level)
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        self.logging.addHandler(handler)
        self.db = PostgresConnector(user=settings.SERVER)
        self.allowed_queues = queues

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
                SELECT match_id,
                       details,
                       timeline
                FROM %s.match_data
                WHERE details IS NOT NULL
                AND timeline IS NOT NULL
                AND roleml IS NULL
                AND queue IN (%s)
                AND duration >= 60 * 12
                LIMIT $1;
                """
                % (settings.SERVER, ",".join(self.allowed_queues)),
                settings.BATCH_SIZE,
            )
            return tasks

    async def update_db(self, results):
        """Update matches in the db."""
        async with self.db.get_connection() as db:
            await db.executemany(
                """
                UPDATE  %s.match_data
                SET roleml = $1
                WHERE match_id = $2
            """
                % settings.SERVER,
                results,
            )

    async def run(self):
        empty = False
        try:
            while not self.stopped:
                tasks = await self.get_tasks()
                if len(tasks) == 0:
                    if not empty:
                        self.logging.info("Found no tasks, Sleeping")
                        empty = True
                    await asyncio.sleep(15)
                    continue
                empty = False
                results = []
                for task in tasks:
                    try:
                        results.append(
                            [
                                json.dumps(
                                    roleml.predict(
                                        json.loads(task["details"]),
                                        json.loads(task["timeline"]),
                                    )
                                ),
                                task["match_id"],
                            ]
                        )
                    except (IncorrectMap, MatchTooShort):
                        results.append(["{}", task["match_id"]])
                await self.update_db(results)
                self.logging.info("Predicted %s matches.", len(results))
                await asyncio.sleep(5)

        except Exception as err:
            traceback.print_tb(err.__traceback__)
            self.logging.info(err)


async def main():
    allowed_queues = []
    with open("queues.json", "r") as queue_file:
        queues = json.loads(queue_file.read())
        for queue in queues:
            if queue["map"] == "Summoner's Rift":
                allowed_queues.append(str(queue["queueId"]))
    manager = Manager(allowed_queues)

    def shutdown_handler():
        """Shutdown."""
        manager.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    await manager.run()


if __name__ == "__main__":
    asyncio.run(main())
