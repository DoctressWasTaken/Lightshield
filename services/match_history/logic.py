"""Match History updater. Pulls matchlists for all player."""
import asyncio
import os
from exceptions import RatelimitException, NotFoundException, Non200Exception
from worker import WorkerClass
from service import ServiceClass
from datetime import datetime


class Service(ServiceClass):
    timelimit = None

    async def init(self):
        """Initiate timelimit for pulled matches."""
        try:
            self.timelimit = int(os.environ['TIME_LIMIT'])
        except:
            pass
        try:
            self.required_matches = int(os.environ['MATCHES_TO_UPDATE'])
        except:
            pass


class Worker(WorkerClass):

    async def process_task(self, session, content):

        identifier = content['summonerId']

        new_matches = matches = content['wins'] + content['losses']
        if prev := await self.service.marker.execute_read(
                'SELECT matches FROM match_history WHERE summonerId = %s;' % identifier):
            print(prev)
            new_matches = matches - int(prev[0])
        elif prev := await self.service.redisc.hgetall(f"user:{identifier}"):
            temp = (int(prev['wins']) + int(prev['losses']))
            new_matches = matches - temp
            await self.service.marker.execute_write(
                'INSERT INTO match_history (summonerId, matches) VALUES (%s, %s);' % (identifier,
                                                                                      temp))
            await self.service.redisc.hdel(f'user:{identifier}')

        if new_matches < self.service.required_matches:  # Skip if less than required new matches
            # TODO: Despite not having enough matches this should be considered to pass on to the db
            return

        if identifier in self.service.buffered_elements:
            return None

        self.service.buffered_elements[identifier] = True

        matches_to_call = matches + 3
        calls = int(matches_to_call / 100) + 1
        ids = [start_id * 100 for start_id in range(calls)]
        calls_in_progress = []
        while ids:
            id = ids.pop()
            calls_in_progress.append(asyncio.create_task(
                self.handler(
                    session=session,
                    url=self.service.url % (content['accountId'], id, id + 100)
                )
            ))
            await asyncio.sleep(0.1)
        try:
            responses = await asyncio.gather(*calls_in_progress)
            match_data = list(set().union(*responses))
            await self.service.marker.execute_write(
                'UPDATE match_history SET matches = %s WHERE summonerId =  %s;' % (matches,
                                                                                   identifier))
            while match_data:
                id = match_data.pop()
                await self.service.add_package({"match_id": id})
        except NotFoundException:
            pass
        finally:
            del self.service.buffered_elements[identifier]

    async def handler(self, session, url):
        rate_flag = False
        while not self.service.stopped:
            if datetime.now() < self.service.retry_after or rate_flag:
                rate_flag = False
                delay = max(0.5, (self.service.retry_after - datetime.now()).total_seconds())
                await asyncio.sleep(delay)
            try:
                response = await self.service.fetch(session, url)
                if not self.service.timelimit:
                    return [match['gameId'] for match in response['matches'] if
                            match['queue'] == 420 and
                            match['platformId'] == self.service.server]
                return [match['gameId'] for match in response['matches'] if
                        match['queue'] == 420 and
                        match['platformId'] == self.service.server and
                        int(str(match['timestamp'])[:10]) >= self.service.timelimit]

            except RatelimitException:
                rate_flag = True
            except Non200Exception:
                await asyncio.sleep(0.1)
