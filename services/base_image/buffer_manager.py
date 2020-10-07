import asyncio
import logging
import aioredis
import json

class BufferManager():
    """Returns and adds new elements to the buffers.

    This class provides connection between logic and buffer.
    """

    def __init__(self):
        self.logging = logging.getLogger("BufferManager")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.redisc = None


    async def init(self) -> None:
        """Initiate async elements."""
        self.redisc = await aioredis.create_redis_pool(
            ('redis', 6379), db=0, encoding='utf-8')


    async def get_task(self):
        """Return a task if exists."""
        if task := await self.redisc.lpop('tasks'):
            task = json.loads(task)
        return task

    async def add_package(self, package):
        """Add an outgoing package."""

        return await self.redisc.lpush('packages', json.dumps(package))

    async def get_total_packages(self):
        """Method required for league_rankings.

        Determines if tasks should be paused due to too many remaining outputs.
        """
        return await self.redisc.llen('packages')