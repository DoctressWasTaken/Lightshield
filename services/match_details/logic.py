"""Match History updater. Pulls matchlists for all player."""
from exceptions import RatelimitException, NotFoundException, Non200Exception
from worker import WorkerClass
from service import ServiceClass
import json
import asyncio


class Service(ServiceClass):

    async def init(self):
        await self.marker.connect()
        self.limiter_task = asyncio.create_task(self.limiter())

    async def limiter(self):
        """Method to periodically break down the db size by removing a % of the lowest match Ids."""
        retain_period_days = 60
        removal = 100/retain_period_days
        while True:
            await asyncio.sleep(24*60*60)
            count = await self.marker.execute_read(
                'SELECT COUNT(*) FROM match_id'
            )
            to_delete = int(count * removal)
            lowest_limit = await self.marker.execute_read(
                'SELECT id FROM match_id ORDER BY id ASC LIMIT %s' % to_delete
            )[-1]
            await self.marker.execute_read(
                'DELETE FROM match_id WHERE id <= %s' % to_delete
            )


class Worker(WorkerClass):

    async def process_task(self, session, content):

        identifier = content['match_id']
        if data := await self.service.marker.execute_read(
                'SELECT * FROM match_id WHERE id = %s;' % identifier):
            return
        if identifier in self.service.buffered_elements:
            return
        self.service.buffered_elements[identifier] = True
        url = self.service.url % identifier
        try:
            response = await self.service.fetch(session, url)
            await self.service.marker.execute_write(
                'INSERT OR IGNORE INTO match_id (id) VALUES (%s);' % identifier)
            await self.service.add_package(response)
        except (RatelimitException, Non200Exception):
            await self.service.redisc.lpush('tasks', json.dumps(content))  # Return to task queue
        except NotFoundException:
            pass
        finally:
            del self.service.buffered_elements[identifier]
