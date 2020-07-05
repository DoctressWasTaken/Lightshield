"""Match History updater. Pulls matchlists for all player."""
from exceptions import RatelimitException, NotFoundException, Non200Exception
from worker import WorkerClass
import json

class Worker(WorkerClass):

    async def process_task(self, session, content):

        identifier = content['match_id']
        if prev := await self.service.redisc.sismember('matches', str(identifier)):
            return
        if identifier in self.service.buffered_elements:
            return
        self.service.buffered_elements[identifier] = True
        url = self.service.url % identifier
        try:
            response = await self.service.fetch(session, url)
            await self.service.redisc.sadd('matches', identifier)
            await self.service.add_package(response)
        except (RatelimitException, Non200Exception):
            await self.service.redisc.lpush('tasks', json.dumps(content))  # Return to task queue
        except NotFoundException:
            pass
        finally:
            del self.service.buffered_elements[identifier]
