"""Summoner ID Updater  - Logical elements.

No Service defined as the service is exactly the same as the default case.
Import is done directly.
"""
from exceptions import RatelimitException, NotFoundException, Non200Exception
from worker import WorkerClass
from service import ServiceClass

class Service(ServiceClass):

    async def init(self):
        await self.marker.connect()


class Worker(WorkerClass):
    """Worker processing Summoner IDs."""

    async def process_task(self, session, task):
        """Create only a new call if the summoner is not yet in the db."""
        identifier = task['summonerId']
        if data := await self.service.marker.execute_read(
                'SELECT accountId, puuid FROM summoner_ids WHERE summonerId = "%s";' % identifier):
            package = {**task, 'accountId': data[0][0], 'puuid': data[0][1]}

            await self.service.add_package(package)
            return
        if identifier in self.service.buffered_elements:
            return
        self.service.buffered_elements[identifier] = True
        url = self.service.url % identifier
        try:
            response = await self.service.fetch(session, url)
            await self.service.marker.execute_write(
                'INSERT INTO summoner_ids (summonerId, accountId, puuid) '
                'VALUES ("%s", "%s", "%s");' % (
                identifier, response['accountId'], response['puuid']))

            await self.service.add_package({**task, **response})
        except (RatelimitException, NotFoundException, Non200Exception):
            return
        finally:
            del self.service.buffered_elements[identifier]
