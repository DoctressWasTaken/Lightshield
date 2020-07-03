"""League Updater Module."""
import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timedelta
import aiohttp
import aio_pika
import aioredis
from aio_pika import Message
from aio_pika.pool import Pool
from rank_manager import RankManager


class Worker:  # pylint: disable=R0902
    """Core service worker object."""

    def __init__(self, parallel_worker):
        """Initiate sync elements on creation."""
        self.rankmanager = RankManager()

        self.server = os.environ['SERVER']
        self.url = f"http://{self.server}.api.riotgames.com/lol" \
                   f"/league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        self.retry_after = datetime.now()
        self.max_worker = parallel_worker

        self.empty = False
        self.next_page = 1
        self.page_entries = []

        self.logging = logging.getLogger("Core")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [CORE] %(message)s'))
        self.logging.addHandler(handler)
        self.redis = None
        self.host = 'redis'
        self.port = 6379
        self.stopping = False

        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self):
        """Set shutdown flag."""
        self.logging.info("Received shutdown signal. Shutting down.")
        self.stopping = True

    async def init(self):
        """Initiate async elements during runtime."""
        self.redis = await aioredis.create_redis_pool(
            (self.host, self.port), db=0, encoding='utf-8')

    async def check_new(self, entry):
        """Check received element for changes compared to cached info.

        Does return false if no difference in wins/losses has occurred.
        Else True.
        """
        hash_db = await self.redis.get(entry['summonerId'])
        hash_local = f"{entry['wins']}_{entry['losses']}"
        if hash_db == hash_local:
            return False
        await self.redis.set(entry['summonerId'], hash_local)
        return True

    async def filter_data(self):
        """Remove unchanged summoners."""
        self.logging.info("Filtering %s entries.", len(self.page_entries))
        self.page_entries = [entry for entry in self.page_entries if await self.check_new(entry)]

    async def push_data(self):
        """Send out gathered data via rabbitmq tasks."""
        if not self.page_entries:
            self.logging.info("No changes to push. Skipping")
            return
        rabbit = await aio_pika.connect_robust('amqp://guest:guest@rabbitmq/')
        # Outgoing
        channel = await rabbit.channel()
        await channel.set_qos(prefetch_count=1)

        rabbit_exchange_out = await channel.declare_exchange(
            name=f'LEAGUE_OUT_{self.server}',
            type='direct',
            durable=True)
        summoner_in = await channel.declare_queue(
            name=f'SUMMONER_ID_IN_{self.server}',
            durable=True)
        await summoner_in.bind(rabbit_exchange_out, 'SUMMONER_V1')
        self.logging.info("Pushing %s summoner.", len(self.page_entries))
        loop = asyncio.get_event_loop()

        async def get_connection():
            """Create connection"""
            return await aio_pika.connect_robust("amqp://guest:guest@rabbitmq/")
        connection_pool = Pool(get_connection, max_size=3, loop=loop)

        async def get_channel() -> aio_pika.Channel:
            """Create channel."""
            async with connection_pool.acquire() as connection:
                return await connection.channel()

        channel_pool = Pool(get_channel, max_size=15, loop=loop)

        async def publish(entry):
            """Publish to channel."""
            async with channel_pool.acquire() as channel:
                rabbit_exchange_out = await channel.declare_exchange(
                    name=f'LEAGUE_OUT_{self.server}',
                    type='direct',
                    durable=True)
                await rabbit_exchange_out.publish(
                    message=Message(
                        bytes(json.dumps(entry), 'utf-8')),
                    routing_key='SUMMONER_V1')

        await asyncio.wait([publish(entry) for entry in self.page_entries])

    async def worker(self, tier, division):
        """Create and execute calls until one of the multiple worker returns an empty page.

        Pages that failed their call are retried.
        Worker don't exit until their failed page is resolved.
        """
        failed = None
        while (not self.empty or failed) and not self.stopping:

            if self.retry_after > datetime.now():
                delay = (self.retry_after - datetime.now()).total_seconds()
                await asyncio.sleep(delay)
            if not failed and len(self.page_entries) < 1000:
                page = self.next_page
                self.next_page += 1
            elif failed:
                page = failed
                failed = None
            else:
                return
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        url=self.url % (tier, division, page),
                        proxy="http://proxy:8000") as response:
                    try:
                        resp = await response.json(content_type=None)
                    except aiohttp.ClientConnectionError:
                        pass
                    if response.status in [429, 430]:
                        if "Retry-After" in response.headers:
                            delay = max(int(response.headers['Retry-After']), 1)
                            self.retry_after = datetime.now() + timedelta(seconds=delay)
                    if response.status != 200:
                        failed = page
                    else:
                        if len(resp) == 0:
                            self.logging.info("Empty page %s found.", page)
                            self.empty = True
                        else:
                            self.page_entries += resp

    async def release_messaging_queue(self):
        """Interrupt application until the target message queue falls below a certain threshhold."""
        headers = {
            'content-type': 'application/json'
            }
        out = False
        while not self.stopping:
            async with aiohttp.ClientSession(auth=aiohttp.BasicAuth("guest", "guest")) as session:
                async with session.get(
                        'http://rabbitmq:15672/api/queues', headers=headers) as response:
                    resp = await response.json()
                    queues = {entry['name']: entry for entry in resp}
                    if int(queues[f'SUMMONER_ID_IN_{self.server}']['messages']) < 1000:
                        return
                    if not out:
                        self.logging.info('Awaiting messages to be reduced.')
                        out = True
                    await asyncio.sleep(5)

    async def main(self):
        """Manage ranks to call and worker start/stops."""
        await self.rankmanager.init()
        await self.init()
        for _ in range(await self.rankmanager.get_total()):
            await self.release_messaging_queue()
            tier, division = await self.rankmanager.get_next()
            self.empty = False
            self.next_page = 1
            self.page_entries = []
            while not self.empty and not self.stopping:
                await asyncio.gather(*[asyncio.create_task(
                    self.worker(tier=tier, division=division)) for __ in range(self.max_worker)])
                if self.page_entries:
                    await self.filter_data()
                    await self.push_data()
                    self.page_entries = []
                await self.release_messaging_queue()

            await self.rankmanager.update(key=(tier, division))


async def main():
    """Start loop to request data from the api and update the DB.

    The loop is limited to run once every 6 hours max.
    """
    while True:
        await asyncio.gather(
            Worker(parallel_worker=int(os.environ['BUFFER'])).main(),
            asyncio.sleep(3600 * int(os.environ['UPDATE_INTERVAL']))
        )

if __name__ == "__main__":
    asyncio.run(main())
