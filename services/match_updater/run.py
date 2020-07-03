"""Match History updater. Pulls matchlists for all player."""

import asyncio
from worker import WorkerClass, ServiceClass

from exceptions import (
    RatelimitException,
    NotFoundException,
    Non200Exception,
    NoMessageException
)

from db_connector import Worker as DBWorker


class MatchUpdater(ServiceClass):

    def set_task_holder(self):
        self.task_holder = []

    async def init(self):
        self.logging.info("Initiating Service.")
        channel = await self.rabbitc.channel()
        # Incoming
        incoming = await channel.declare_queue(
            'MATCH_IN_' + self.server, durable=True)


class Worker(WorkerClass):

    async def init(self):
        await self.channel.set_qos(prefetch_count=15)
        self.incoming = await self.channel.declare_queue(
            'MATCH_IN_' + self.service.server, durable=True)

    async def get_task(self):
        try:
            while not (msg := await self.incoming.get(no_ack=False, fail=False)) \
                    and not self.service.stopping:
                await asyncio.sleep(0.1)
        except asyncio.exceptions.TimeoutError:
            self.logging.info("TimeoutError")
            await asyncio.sleep(0.2)
            return None
        if not msg:
            return None
        identifier = msg.body.decode('utf-8')

        if prev := await self.service.redisc.sismember('matches', str(identifier)):
            await msg.ack()
            return None

        if identifier in self.service.buffered_elements:
            await msg.ack()
            return None

        self.service.buffered_elements[identifier] = True
        return [identifier, msg]

    async def process_task(self, session, data):
        identifier, msg = data
        url = self.service.url % identifier

        try:
            response = await self.fetch(session, url)
            await self.service.redisc.sadd('matches', identifier)
            self.service.task_holder.append(response)
            await msg.ack()
        except (RatelimitException, Non200Exception):
            await msg.reject(requeue=True)
            # self.logging.info("Rejecting + ")
        except NotFoundException:
            await msg.reject(requeue=False)
            # self.logging.info("Rejecting")
        finally:
            del self.service.buffered_elements[identifier]
        if identifier in self.service.buffered_elements:
            self.logging.info("This one was not properly removed")


if __name__ == "__main__":
    service = MatchUpdater(
        url_snippet="match/v4/matches/%s",
        queues_out=[])
    service.set_task_holder()
    worker = DBWorker(service)
    worker.start()
    asyncio.run(service.run(Worker))
