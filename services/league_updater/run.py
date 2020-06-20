"""League Updater Module."""
import asyncio
import json
import logging
import threading
import os
import datetime, time
from datetime import datetime, timedelta
import pika
import websockets
import aiohttp
import aio_pika
from aio_pika import Message
from aio_pika import DeliveryMode

from rank_manager import RankManager

class EmptyPageException(Exception):
    """Custom Exception called when at least 1 page is empty."""

    def __init__(self, success, failed):
        """Accept response data and failed pages."""
        self.success = success
        self.failed = failed


logging.basicConfig(
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.INFO)

logging.getLogger("pika").setLevel(logging.WARNING)

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

    async def main(self):
        for rank in range(self.rankmanager.get_total()):
            tier, division = await self.rankmanager.get_next()

            self.empty = False
            self.next_page = 1
            self.page_entries = []

            await asyncio.gather(*[
                asyncio.create_task(self.worker(id=i, tier=tier, division=division)) for i in range(self.max_worker)])

            await self.push_data()

            await self.rankmanager.update(key=(tier, division))

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
        self.page_entries = list(set(self.page_entries))
        for entry in self.page_entries:
            await rabbit_exchange_out.publish(
                message=Message(
                    bytes(json.dumps(entry), 'utf-8'),
                    delivery_mode=DeliveryMode.PERSISTENT),
                routing_key='SUMMONER_V1')

    async def worker(self, id, tier, division):
        """Call and process page data. Multiple are started and work until pages return empty."""
        log = logging.getLogger("Worker_" + str(id))
        log.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [Worker {id}] %(message)s'))
        log.addHandler(ch)
        log.info("Initiated.")

        failed = None
        while not self.empty or failed:
            if self.retry_after > datetime.now():
                delay = (self.retry_after - datetime.now()).total_seconds()
                if delay > 10:
                    log.info(f"Sleeping for {delay}.")
                await asyncio.sleep(delay)
            async with aiohttp.ClientSession() as session:
                if not failed:
                    page = self.next_page
                    self.next_page += 1
                else:
                    page = failed
                    failed = None
                async with session.get(url=self.url % (tier, division, page), proxy="http://proxy:8000") as response:
                    try:
                        resp = await response.json(content_type=None)
                    except:
                        print(await response.text())
                    if response.status in [429, 430]:
                        if "Retry-After" in response.headers:
                            delay = int(response.headers['Retry-After'])
                            self.retry_after = datetime.now() + timedelta(seconds=delay)
                    if response.status != 200:
                        failed = page
                    else:
                        if len(resp) == 0:
                            log.info(f"Empty page {page} found.")
                            self.empty = True
                        else:
                            self.page_entries += resp
        log.info("Exited.")


async def main():
    """Start loop to request data from the api and update the DB.

    The loop is limited to run once every 6 hours max.
    """
    worker = Worker(parallel_worker=5)
    while True:
        await asyncio.gather(
            worker.main(),
            asyncio.sleep(3600 * 3)  # 3 Hour sleep period
        )


if __name__ == "__main__":

    asyncio.run(main())