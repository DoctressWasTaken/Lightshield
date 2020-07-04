"""Match History updater. Pulls matchlists for all player."""
from exceptions import RatelimitException, NotFoundException, Non200Exception
from worker import WorkerClass


class Worker(WorkerClass):

    async def process_task(self, session, content):

        identifier = content['matchId']
        if prev := await self.service.redisc.sismember('matches', str(identifier)):
            return
        if identifier in self.service.buffered_elements:
            return
        self.service.buffered_elements[identifier] = True
        url = self.service.url % identifier
        try:
            response = await self.fetch(session, url)
            await self.service.redisc.sadd('matches', identifier)
            self.service.add_package(response)
        except (RatelimitException, Non200Exception):
            await self.service.redisc.lpush('tasks', content)  # Return to task queue
        except NotFoundException:
            pass
        finally:
            del self.service.buffered_elements[identifier]
