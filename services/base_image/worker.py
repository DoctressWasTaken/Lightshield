"""Default worker module containing the Worker class."""
import asyncio
from datetime import datetime, timedelta
import aiohttp


class WorkerClass:
    """Worker default class.

    Multiple worker are initiated by the ServiceClass.
    """
    def __init__(self, parent_service):
        """Pass relevant data to worker.

        ::param service: Parent service object.
        """
        self.service = parent_service
        self.logging = self.service.logging

    async def init(self):
        """Initiate worker.

        Abstract method replaced by the worker.
        """

    async def process_task(self, session, task):  # pylint: disable=W0613
        """Process Task.

        Abstract method. Processes the received task.
        """
        await asyncio.sleep(0.5)
        self.logging.info("Received %s.", task)

    async def run(self):
        """Handle the core worker loop.

        Waits for task received, processes task. Interrupts if the outgoing queue is blocked or the
        API returns a ratelimit issue.
        """
        await self.init()
        async with aiohttp.ClientSession() as session:
            while not self.service.stopped:
                if (delay := (self.service.retry_after - datetime.now()).total_seconds()) > 0:
                    if delay > 5:
                        self.logging.info("Sleeping for %s.", delay)
                    await asyncio.sleep(delay)
                while self.service.local_buffer_full:
                    await asyncio.sleep(0.5)
                if task := await self.service.get_task():
                    await self.process_task(session, task)

        self.logging.info("Exiting worker")
