"""League Updater Module."""
import asyncio
from datetime import datetime
import aiohttp
import aioredis
from rank_manager import RankManager

from service import ServiceClass
from worker import WorkerClass
from exceptions import RatelimitException, NotFoundException, Non200Exception


class Service(ServiceClass):  # pylint: disable=R0902
    """Core service worker object."""

    def __init__(self):
        """Initiate sync elements on creation."""
        super().__init__()

        self.rankmanager = RankManager()

        self.empty = False
        self.next_page = 1

    async def init(self):
        """Override of the default init function.

        Initiate the Rankmanager object.
        """
        self.redisc = await aioredis.create_redis_pool(
            ('redis', 6379), db=0, encoding='utf-8')

        await self.rankmanager.init()

    async def run(self, Worker):
        """Override the default run method due to special case.

        Worker are started and stopped after each tier/rank combination.
        """
        await self.init()
        workers = [Worker(self) for i in range(self.parallel_worker)]

        while not self.service.stopped:
            tier, division = await self.rankmanager.get_next()

            self.empty = False
            self.next_page = 1

            await asyncio.gather(
                self.check_local_buffer(),
                *[worker.run(tier=tier, division=division) for worker in workers]
            )

            await self.rankmanager.update(key=(tier, division))


class Worker(WorkerClass):

    async def run(self, tier, division):
        """Override of the default run method.

        :param tier: Currently called tier.
        :param division: Currently called division.
        """
        failed = None
        while (not self.service.empty or failed) and not self.service.stopped:

            if (delay := (self.service.retry_after - datetime.now()).total_seconds()) > 0:
                await asyncio.sleep(delay)
            while not self.service.stopped and self.service.local_buffer_full:
                await asyncio.sleep(0.5)
            if self.service.stopped:
                return

            if not failed:
                page = self.service.next_page
                self.service.next_page += 1
            else:
                page = failed
                failed = None
            async with aiohttp.ClientSession() as session:
                try:
                    content = await self.service.fetch(session, url=self.service.url % (
                    tier, division, page))
                    if len(content) == 0:
                        self.service.empty = True
                        return
                    await self.process_task(content)
                except (RatelimitException, Non200Exception):
                    failed = page
                except NotFoundException:
                    self.service.empty = True

    async def process_task(self, content) -> None:
        """Process the received list of summoner.

        Check for changes that would warrent it be sent on.
         """
        for entry in content:
            hash_db = await self.service.redisc.get(entry['summonerId'])
            hash_local = f"{entry['wins']}_{entry['losses']}"
            if hash_db == hash_local:
                return
            await self.service.redisc.set(entry['summonerId'], hash_local)
            await self.service.add_package(entry)
