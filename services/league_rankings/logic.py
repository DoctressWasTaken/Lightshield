"""League Updater Module."""
import asyncio
from datetime import datetime
import os
import aiohttp
import aioredis
from rank_manager import RankManager

from service import ServiceClass
from worker import WorkerClass
from exceptions import RatelimitException, NotFoundException, Non200Exception


class Service(ServiceClass):  # pylint: disable=R0902
    """Core service worker object."""

    def __init__(self, *args, **kwargs):
        """Initiate sync elements on creation."""
        super().__init__(*args, **kwargs)

        self.rankmanager = RankManager()

        self.empty = False
        self.next_page = 1

    async def init(self):
        """Override of the default init function.

        Initiate the Rankmanager object.
        """
        self.redisc = await aioredis.create_redis_pool(
            ('redis', 6379), db=0, encoding='utf-8')
        await self.marker.connect()
        await self.rankmanager.init()

    async def run(self, Worker):
        """Override the default run method due to special case.

        Worker are started and stopped after each tier/rank combination.
        """
        await self.init()
        workers = [Worker(self) for i in range(self.parallel_worker)]
        buffer_check = asyncio.create_task(self.check_local_buffer())
        while not self.stopped:
            tier, division = await self.rankmanager.get_next()

            self.empty = False
            self.next_page = 1

            await asyncio.gather(
                *[worker.run(tier=tier, division=division) for worker in workers]
            )
            await self.rankmanager.update(key=(tier, division))
        await buffer_check

    async def check_local_buffer(self) -> None:
        """Set local_buffer_full flag to pause local calls."""
        count = 0
        self.logging.info("Initiating buffer output.")
        while not self.stopped:
            count += 1
            if await self.redisc.llen('packages') > self.max_local_buffer:
                self.local_buffer_full = True
            else:
                self.local_buffer_full = False
            await asyncio.sleep(1)
            if count == 15:
                self.logging.info(
                    "Outgoing Queue: %s/%s, Output since last: %s",
                    await self.redisc.llen('packages'),
                    self.max_local_buffer,
                    os.environ['OUTGOING'])
                os.environ['OUTGOING'] = "0"
                count = 0


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
                        self.logging.info("Page %s is empty.", page)
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
            matches_local = entry['wins'] + entry['losses']
            matches = None
            if prev := await self.service.marker.execute_read(
                    'SELECT matches FROM match_history WHERE summonerId = "%s";' % entry['summonerId']):
                matches = int(prev[0][0])
                
            if matches and matches == matches_local:
                return
            await self.service.marker.execute_write(
                'UPDATE match_history SET matches = %s WHERE summonerId = "%s";' % (
                matches_local,
            entry['summonerId']))

            await self.service.add_package(entry)
