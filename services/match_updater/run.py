"""Match History updater. Pulls matchlists for all player."""

import asyncio
import uvloop
import signal
import os

if "SERVER" not in os.environ:
    print("No SERVER env variable provided. exiting")
    exit()

os.environ['STATUS'] = "RUN"

from worker import (
    Worker,
    RatelimitException,
    NotFoundException,
    Non200Exception,
    NoMessageException
)

def end_handler(sig, frame):

    os.environ['STATUS'] = "STOP"


class MatchUpdater(Worker):

    async def initiate_pika(self, connection):

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        # Incoming
        incoming = await channel.declare_queue(
            'MATCH_IN_' + self.server, durable=True)
        # Outgoing
        outgoing = await channel.declare_exchange(
            f'MATCH_OUT_{self.server}', type='direct',
            durable=True)
        db_in = await channel.declare_queue(
            'DB_MATCH_IN_' + self.server, durable=True)
        await db_in.bind(outgoing, 'MATCH')

        await self.pika.init(incoming=incoming, outgoing=outgoing, tag='MATCH')

    async def is_valid(self, identifier, content, msg):

        if prev := await self.redis.sismember(set='matches', key=identifier):
            await self.pika.ack(msg)
            return False
        return {"foo": "bar"}

    async def worker(self, session, identifier, msg, **kwargs):
        url = self.url_template % (identifier)
        self.logging.debug(f"Fetching {url}")
        try:
            response = await self.fetch(session, url)
            await self.redis.sadd(
                set='matches',
                key=identifier)
            await self.pika.push(response)
            await self.pika.ack(msg)
        except RatelimitException:
            pass
        except NotFoundException:
            await self.pika.reject(msg, requeue=False)
        except Non200Exception:
            await self.pika.reject(msg, requeue=True)
        finally:
            del self.buffered_elements[identifier]


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, end_handler)
    uvloop.install()
    buffer = int(os.environ['BUFFER'])
    worker = MatchUpdater(
        buffer=buffer,
        url=f"http://{os.environ['SERVER']}.api.riotgames.com/lol/match/v4/matches/%s",
        identifier=None)
    asyncio.run(worker.main())
