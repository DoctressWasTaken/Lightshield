"""League Updater Module."""
import asyncio
import json
import logging
import threading
import os
import datetime, time
import pika
import websockets
import aiohttp

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

url = f"http://{server}.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"

tiers = [
    "IRON",
    "BRONZE",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "DIAMOND",
    "MASTER",
    "GRANDMASTER",
    "CHALLENGER"]

divisions = [
    "IV",
    "III",
    "II",
    "I"]

map_tiers = {
    'IRON': 0,
    'BRONZE': 4,
    'SILVER': 8,
    'GOLD': 12,
    'PLATINUM': 16,
    'DIAMOND': 20,
    'MASTER': 24,
    'GRANDMASTER': 24,
    'CHALLENGER': 24}
map_divisions = {
    'IV': 0,
    'III': 1,
    'II': 2,
    'I': 3}

active_tasks = 0
summoner = []
failed_pages = []  # Pages that were called but returned a non 200 response
retry_after = 0
empty = None  # Set to the page that is returned empty

async def fetch(url, session, page):
    """Fetch data from the api.

    Returns Non-200 HTTP Code on error.
    """
    global active_tasks
    global failed_pages
    global summoner
    global retry_after
    global empty
    print("Starting with page", page)
    active_tasks += 1
    async with session.get(url, proxy="http://proxy:8000") as response:
        try:
            resp = await response.json(content_type=None)
        except:
            print(await response.text())
        print("Got", response.status)
        if response.status in [429, 430]:
            if "Retry-After" in response.headers:
                retry_after = int(response.headers['Retry-After'])
        if response.status != 200:
            failed_pages.append(page)
        else:
            if len(resp) == 0:
                print("Page", page, "empty.")
                empty = page
            else:
                summoner += resp
    print("Done with page", page)

    active_tasks -= 1


async def update(tier, division, buffer):
    """Get data.

    Core function that sets up call batches by rank category.
    """
    global active_tasks
    global failed_pages
    global retry_after
    global empty
    empty = False
    global summoner
    summoner = []

    next_page = 1
    task_list = []
    async with aiohttp.ClientSession() as session:
        while not empty or failed_pages:
            while active_tasks < buffer:
                if retry_after > 0:
                    await asyncio.sleep(retry_after)
                    retry_after = 0

                if failed_pages:
                    page = failed_pages.pop()
                elif not empty:
                    page = next_page
                    next_page += 1
                else:
                    break

                task_list.append(
                    asyncio.create_task(fetch(url % (tier, division, page), session, page))
                )
                await asyncio.sleep(0.5)
            await asyncio.sleep(0.5)

        await asyncio.gather(*task_list)
    print("Returning", len(summoner), "Summoner.")
    return summoner


def server_updater():
    """Iterate through all rankings and initiate calls."""
    for tier in reversed(tiers):
        for division in divisions:
            buffer = 8
            if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                buffer = 3
                if division != "I":
                    continue
            print(f"Starting with {tier} {division}.")
            try:
                credentials = pika.PlainCredentials('guest', 'guest')
                parameters = pika.ConnectionParameters(
                    'rabbitmq', 5672, '/', credentials)
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                # Outgoing
                channel.exchange_declare(  # Exchange that outputs
                    exchange=f'LEAGUE_OUT_{server}',
                    exchange_type='direct',
                    durable=True
                )
                # Persistent Queues that need to exist
                queue = channel.queue_declare(
                    queue=f'SUMMONER_ID_IN_{server}',
                    durable=True)
                channel.queue_bind(
                    exchange=f'LEAGUE_OUT_{server}',
                    queue=queue.method.queue,
                    routing_key=f'SUMMONER_V1')
                sleep = 1


                received_summoner = asyncio.run(
                    update(tier, division, buffer))


                while received_summoner:
                    summoner = received_summoner.pop()
                    channel.basic_publish(
                        exchange=f'LEAGUE_OUT_{server}',
                        routing_key='SUMMONER_V1',
                        body=json.dumps(summoner),
                        properties=pika.BasicProperties(
                            delivery_mode=2))

            except EmptyPageException as e:
                # add results, if no failed remaining exit
                empty = True
                data += e.success
                failed_pages += e.failed

            except RuntimeError:
                # Raised when rabbitmq cant connect
                print("Failed to reach rabbitmq")
                time.sleep(1)


def main():
    """Start loop to request data from the api and update the DB.

    The loop is limited to run once every 6 hours max.
    """
    while True:

        blocked_till = datetime.datetime.now() + datetime.timedelta(hours=6)
        server_updater()
        while datetime.datetime.now() < blocked_till:
            time.sleep(30)


if __name__ == "__main__":
    main()
