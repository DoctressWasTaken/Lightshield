"""Summoner ID Updater  - Logical elements.

No Service defined as the service is exactly the same as the default case.
Import is done directly.
"""
from exceptions import RatelimitException, NotFoundException, Non200Exception
from worker import WorkerClass


class Worker(WorkerClass):
    """Worker processing Summoner IDs."""

    async def process_task(self, session, task):
        """Create only a new call if the summoner is not yet in the db."""
        identifier = task['summonerId']
        if redis_entry := await self.service.redisc.hgetall(f"user:{identifier}"):
            package = {**task, **redis_entry}
            await self.service.add_package(package)
            return
        if identifier in self.service.buffered_elements:
            return
        self.service.buffered_elements[identifier] = True
        url = self.service.url % identifier
        try:
            response = await self.service.fetch(session, url)
            await self.service.redisc.hmset_dict(
                f"user:{identifier}",
                {'puuid': response['puuid'],
                 'accountId': response['accountId']})
            await self.service.add_package({**task, **response})
        except (RatelimitException, NotFoundException, Non200Exception):
            return
        finally:
            del self.service.buffered_elements[identifier]
