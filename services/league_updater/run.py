"""League Updater Module."""
import asyncio
import json
import logging
import threading
import os
import datetime, time
import pika
import websockets

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

url = "/league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"

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
    loop = asyncio.get_event_loop()
    logging.info(f"Active Threads: {threading.active_count()}")
    call_tasks = []
    for id, task in enumerate(tasks):
        call_tasks.append([id, tier, division, task])
    message = {
        "endpoint": "league-exp",
        "url": url,
        "tasks": call_tasks
    }
    failed = []
    data_sets = []
    empty = False
    print("Making calls. Total:", len(call_tasks))
    async with websockets.connect("ws://proxy:6789") as websocket:
        remaining = len(call_tasks)
        await websocket.send("League_Updater")
        await websocket.send(json.dumps(message))
        while remaining > 0:
            msg = await websocket.recv()
            data = json.loads(msg)
            if data['status'] == "rejected":
                for entry in data['tasks']:
                    remaining -= 1
                    failed.append(call_tasks[entry][-1])
            elif data['status'] == 'done':
                for entry in data['tasks']:
                    remaining -= 1
                    if entry['status'] == 200:
                        if len(entry['result']) == 0:
                            empty = True
                        else:
                            data_sets += entry['result']
                    else:
                        failed.append(call_tasks[int(entry['id'])][-1])
                        print(entry['status'])
                        print(entry['headers'])
    if empty:
        raise EmptyPageException(data_sets, failed)
    return data_sets, failed


def server_updater():
    """Iterate through all rankings and initiate calls."""
    for tier in reversed(tiers):
        for division in divisions:
            size = 8
            if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                size = 4
                if division != "I":
                    continue
            print(f"Starting with {tier} {division}.")
            pages = 1
            failed_pages = []
            data = []
            empty = False
            while not empty or failed_pages:
                tasks = []
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

                    while failed_pages and len(tasks) < size:
                        page = failed_pages.pop()
                        tasks.append(page)
                    while not empty and len(tasks) < size:
                        tasks.append(pages)
                        pages += 1
                    print(f"Requesting {tasks}")
                    success, failed = asyncio.run(
                        update(tier, division, tasks))
                    data += success
                    failed_pages += failed

                    while data:
                        summoner = data.pop()
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
