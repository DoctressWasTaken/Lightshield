"""League Updater Module."""
import asyncio
import json
import logging
import threading
import os
import aiohttp
from aiohttp.client_exceptions import ClientConnectorError
import datetime, time
import pika

class EmptyPageException(Exception):
    """Custom Exception called when at least 1 page is empty."""

    def __init__(self, success, failed):
        """Accept response data and failed pages."""
        self.success = success
        self.failed = failed

class ServerConnectionException(Exception):
    """Custom Exception called when at least """



logging.basicConfig(
        format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.INFO)

logging.getLogger("pika").setLevel(logging.WARNING)

if 'SERVER' not in os.environ:
    print("No server provided, exiting.")
    exit()

server = os.environ['SERVER']

url = f"http://proxy:8000/league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"

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


async def fetch(url, session, page):
    """Fetch data from the api.

    Returns Non-200 HTTP Code on error.
    """
    async with session.get(url) as response:
        resp = await response.json(content_type=None)
        return response.status, resp, page


async def update(tier, division, tasks):
    """Get data.

    Core function that sets up call batches by rank category.
    """
    data_sets = []
    loop = asyncio.get_event_loop()
    logging.info(f"Active Threads: {threading.active_count()}")
    for thread in threading.enumerate():
        print(thread)
    call_tasks = []
    async with aiohttp.ClientSession() as session:
        for task in tasks:
            call_tasks.append(
                asyncio.create_task(
                    fetch(
                        url % (tier, division, task),
                        session,
                        task
                    )
                )
            )
        responses = await asyncio.gather(*call_tasks)
    failed = []
    empty = 0
    wait = 0
    for response in responses:
        if response[0] == 428:
            wait = 1
        if response[0] != 200:
            print(response)
            failed.append(response[2])
        else:
            if len(response[1]) == 0:
                empty += 1
            else:
                data_sets.append(response[1])
    if empty:
        raise EmptyPageException(data_sets, failed)
    await asyncio.sleep(wait)

    return data_sets, failed


def server_updater():
    """Iterate through all rankings and initiate calls."""
    for tier in reversed(tiers):
        for division in divisions:
            if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                if division != "I":
                    continue

            pages = 1
            failed_pages = []
            data = []
            empty = False
            while not empty or failed_pages:
                tasks = []
                try:
                    connection = pika.BlockingConnection(
                        pika.ConnectionParameters('rabbitmq'))
                    channel = connection.channel()
                    queue = channel.queue_declare(
                        queue=f'LEAGUE_{server}',
                        durable=True)
                    print(queue.method.message_count)
                    while queue.method.message_count > 500:
                        print("Theres too many messages in the queue, waiting.")
                        time.sleep(1)
                        queue = channel.queue_declare(
                            queue=f'LEAGUE_{server}',
                            durable=True,
                            passive=True)

                    while failed_pages and len(tasks) < 5:
                        page = failed_pages.pop()
                        tasks.append(page)
                    while not empty and len(tasks) < 5:
                        tasks.append(pages)
                        pages += 1
                    print(f"Requesting {tasks}")
                    success, failed = asyncio.run(
                        update(tier, division, tasks))
                    data += success
                    failed_pages += failed
                    
                    while data:
                        page = data.pop()
                        for entry in page:
                            channel.basic_publish(
                                exchange='',
                                routing_key=f'LEAGUE_{server}',
                                body=json.dumps(entry),
                                properties=pika.BasicProperties(delivery_mode=2))

                except ClientConnectorError:
                    # Raised when the proxy cant be reached
                    print("Failed to reach Proxy")
                    failed_pages += tasks
                    time.sleep(1)

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
