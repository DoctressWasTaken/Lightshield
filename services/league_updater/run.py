"""League Updater Module."""
import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
import aiohttp
import aio_pika
from aio_pika import Message
from aio_pika.pool import Pool
from rank_manager import RankManager

class EmptyPageException(Exception):
    """Custom Exception called when at least 1 page is empty."""

    def __init__(self, success, failed):
        """Accept response data and failed pages."""
        self.success = success
        self.failed = failed

if 'SERVER' not in os.environ:
    print("No server provided, exiting.")
    exit()

server = os.environ['SERVER']


class Worker:

    def __init__(self, parallel_worker):
        self.rankmanager = RankManager()
        self.url = f"http://{server}.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"
        self.retry_after = datetime.now()
        self.max_worker = parallel_worker

        self.empty = False
        self.next_page = 1
        self.page_entries = []
        
        self.logging = logging.getLogger("Core")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [CORE] %(message)s'))
        self.logging.addHandler(ch)

    async def push_data(self):
        """Send out gathered data via rabbitmq tasks."""

        rabbit = await aio_pika.connect_robust('amqp://guest:guest@rabbitmq/')
        # Outgoing
        channel = await rabbit.channel()
        await channel.set_qos(prefetch_count=1)

        rabbit_exchange_out = await channel.declare_exchange(
            name=f'LEAGUE_OUT_{server}',
            type='direct',
            durable=True)
        summoner_in = await channel.declare_queue(
            name=f'SUMMONER_ID_IN_{server}',
            durable=True)
        await summoner_in.bind(rabbit_exchange_out, 'SUMMONER_V1')

        self.logging.info(f"Pushing {len(self.page_entries)} summoner.")
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
            async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
                rabbit_exchange_out = await channel.declare_exchange(
                    name=f'LEAGUE_OUT_{server}',
                    type='direct',
                    durable=True)
                await rabbit_exchange_out.publish(
                    message=Message(
                        bytes(json.dumps(entry), 'utf-8')),
                    routing_key='SUMMONER_V1')

        await asyncio.wait([publish(entry) for entry in self.page_entries])
        self.logging.info(f"Done pushing tasks.")

    async def worker(self, tier, division):
        """Create and execute calls until one of the multiple worker returns an empty page.

        Pages that failed their call are retried.
        Worker don't exit until their failed page is resolved.
        """
        failed = None
        while not self.empty or failed:
            if self.retry_after > datetime.now():
                delay = (self.retry_after - datetime.now()).total_seconds()
                await asyncio.sleep(delay)
            async with aiohttp.ClientSession() as session:
                if not failed:
                    page = self.next_page
                    self.next_page += 1
                else:
                    page = failed
                    failed = None
                async with session.get(
                        url=self.url % (tier, division, page),
                        proxy="http://proxy:8000") as response:
                    try:
                        resp = await response.json(content_type=None)
                    except:
                        pass
                    if response.status in [429, 430]:
                        if "Retry-After" in response.headers:
                            delay = max(int(response.headers['Retry-After']), 1)
                            self.retry_after = datetime.now() + timedelta(seconds=delay)
                    if response.status != 200:
                        failed = page
                    else:
                        if len(resp) == 0:
                            self.logging.info(f"Empty page {page} found.")
                            self.empty = True
                        else:
                            self.page_entries += resp

    async def main(self):
        """Manage ranks to call and worker start/stops."""
        await self.rankmanager.init()
        for rank in range(await self.rankmanager.get_total()):
            tier, division = await self.rankmanager.get_next()
            self.empty = False
            self.next_page = 1
            self.page_entries = []
            await asyncio.gather(*[asyncio.create_task(
                self.worker(tier=tier, division=division)) for i in range(self.max_worker)])
            await self.push_data()
            await self.rankmanager.update(key=(tier, division))


async def main():
    """Start loop to request data from the api and update the DB.

    The loop is limited to run once every 6 hours max.
    """
    parallel_workers = int(os.environ['BUFFER'])
    update_interval = int(os.environ['UPDATE_INTERVAL'])
    worker = Worker(parallel_worker=parallel_workers)
    while True:
        await asyncio.gather(
            worker.main(),
            asyncio.sleep(3600 * update_interval)  # 3 Hour sleep period
        )

if __name__ == "__main__":
    asyncio.run(main())
