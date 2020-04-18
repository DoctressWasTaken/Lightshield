"""League updater - Run Module."""
import os
import django
import aiohttp
import asyncio
from datetime import timedelta
from django.utils import timezone
import json
import threading
import queue
import time
config = json.loads(open('../config.json').read())

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "playerdata.settings")
django.setup()
from asgiref.sync import  sync_to_async

from django.conf import settings
from data.models import Player, Page

user = queue.Queue()
servers = ['EUW1', 'KR', 'NA1']

tiers = [
    "IRON",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "DIAMOND",
    "MASTER",
    "GRANDMASTER",
    "CHALLENGER"
]
divisions = [
    "I",
    "II",
    "III",
    "IV"
]
queues = "RANKED_SOLO_5x5"


class WorkerThread(threading.Thread):
    def __init__(self, **kwargs):
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])
        super(WorkerThread, self).__init__()
        self._is_interrupted = False

    def stop(self):
        self._is_interrupted = True

    def run(self):
        pass


class LoadBalancer(WorkerThread):

    min = None
    max = None
    breakpoint = None

    def run(self):
        print("Started Load Balancer")
        workers = []
        while not self._is_interrupted:
            print(f"Currently harbouring {user.qsize()} tasks.")
            if len(workers) < self.min:
                print("Adding needed worker.")
                worker = Worker()
                worker.start()
                workers.append(worker)

            if user.qsize() > self.breakpoint and len(workers) < self.max:
                print(f"Adding extra worker [Now: {len(workers)}].")
                worker = Worker()
                worker.start()
                workers.append(worker)

            if user.qsize() == 0 and len(workers) > self.min:
                print(f"Removing extra worker [Now: {len(workers)}].")
                worker = workers.pop()
                worker.stop()
                worker.join()
            time.sleep(1)

        for worker in workers:
            worker.stop()
        for worker in workers:
            worker.join()

class Worker(WorkerThread):

    def run(self):
        new_player = []
        existing_player = []
        while not self._is_interrupted:
            try:
                element = user.get(timeout=1)
            except:
                time.sleep(1)
                continue
            for player_data in element:
                player = Player.objects.filter(
                        summoner_id=player_data['summonerId'],
                        server=settings.SERVER).first()
                if player:
                    player.update(player_data)
                    existing_player.append(player)
                    continue
                player = Player(
                    summoner_id=player_data['summonerId'],
                    server=settings.SERVER)
                new_player.append(player)
            if len(new_player) > 200:
                Player.objects.bulk_create(new_player)
                new_player = []
            if len(existing_player) > 200:
                Player.objects.bulk_update(
                    existing_player,
                    ['summoner_name',
                     'ranking_solo',
                     'series_solo',
                     'wins_solo',
                     'losses_solo',
                     'update_solo'])
                existing_player = []

        print("Shutting down worker. Committing remaining changes.")
        Player.objects.bulk_create(new_player)
        new_player = []
        Player.objects.bulk_update(
            existing_player,
            ['summoner_name',
             'ranking_solo',
             'series_solo',
             'wins_solo',
             'losses_solo',
             'update_solo'])

def prep():
    """Set up database worker threads and prepare db.

    If no pages are set up declares the initial pages.

    Starts worker, queue and load balancer.
    """
    # Create initial pages if none exist:
    for server in servers:
        if Page.objects.filter(server=server).count() == 0:
            for tier in tiers:
                if tier not in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                    for division in divisions:
                        p = Page(
                            server=server,
                            tier=tier,
                            division=division
                        )
                        p.save()
                else:
                    p = Page(
                        server=server,
                        tier=tier,
                        division="I"
                    )
                    p.save()
        Page.objects.filter(server=server).all().update(last_updated=timezone.now() - timedelta(days=1))

    load_balancer = LoadBalancer(min=1, max=10, breakpoint=50)
    load_balancer.start()

    return load_balancer


async def fetch(url, session, headers):
    """Fetch data and return."""
    print(f"fetching {url}")
    try:
        async with session.get(url, headers=headers) as response:
            resp = await response.json()
            return response.status, resp, response.headers
    except:
        return 999, [], {}

async def server_updater(server, headers):
    """Server specific task started from the main function."""
    divisions = await sync_to_async(list)(Page.objects.filter(server=server).order_by('id').all())
    url = f'https://{server}.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s'

    while True:
        for element in divisions:
            start_page = 1
            forced_timeout = 0
            empty_count = 0
            while empty_count == 0:
                await asyncio.sleep(forced_timeout)
                # calling 9 pages per batch
                tasks = []
                async with aiohttp.ClientSession() as session:
                    for i in range(9):
                        tasks.append(
                            asyncio.create_task(fetch(
                                url % (element.tier, element.division, start_page + i),
                                session,
                                headers=headers
                            )))
                    tasks.append(asyncio.create_task(asyncio.sleep(3)))
                    responses = await asyncio.gather(*tasks)
                for response in responses[:9]:
                    if response[0] == 429:
                        print("Got 429")
                        if 'Retry-After' in response[2]:
                            forced_timeout = int(response[2]['Retry-After'])
                        else:
                            forced_timeout = 1
                        print(f"Waiting an additional {forced_timeout}.")
                    elif response[0] != 200:
                        empty_count = 0
                        start_page += 9
                        break
                    else:
                        if len(response[1]) == 0:
                            empty_count += 1
                        else:
                            await sync_to_async(user.put)(response[1])

                if empty_count == 0:
                    start_page += 9

            element.page_limit = start_page + 9 - empty_count
            await sync_to_async(element.save)()


async def main():
    """Pull data"""
    server_tasks = []

    headers = {'X-Riot-Token': config['API_KEY']}

    for server in servers:
        server_tasks.append(
            asyncio.create_task(server_updater(server, headers))
        )
    await asyncio.gather(*server_tasks)


if __name__ == "__main__":
    load_balancer = prep()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        load_balancer.stop()
    load_balancer.stop()