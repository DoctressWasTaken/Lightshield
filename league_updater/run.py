import os
import django
from datetime import timedelta
from django.utils import timezone
import asyncio
import pika
import json
import threading
import time
config = json.loads(open('../config.json').read())

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "league_updater.settings")
django.setup()

from django.conf import settings
from data.models import Player, Page

tiers = [
    "IRON",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "DIAMOND"
]
divisions = [
    "I",
    "II",
    "III",
    "IV"
]
queues = [
    "RANKED_SOLO_5x5",
    "RANKED_FLEX_SR"
]

class WorkerThread(threading.Thread):
    def __init__(self):
        super(WorkerThread, self).__init__()
        self._is_interrupted = False

    def stop(self):
        self._is_interrupted = True

    def run(self):
        pass


class Listener(WorkerThread):

    def run(self):
        print("Started Listening Worker.")
        connection = pika.BlockingConnection(pika.ConnectionParameters(config['HOST']))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        listen_queue = settings.SERVER + "_LEAGUE_RET"
        print(f"Listening on {listen_queue}")
        channel.queue_declare(queue=listen_queue)

        for message in channel.consume(listen_queue, inactivity_timeout=1):
            if self._is_interrupted:
                print("Received interrupt. Shutting down.")
                break
            if all(x is None for x in message):
                print("No message found")
                continue

            method, properties, body = message
            data = json.loads(body)

            headers = properties.headers
            print(headers)
            page = Page.objects.get(id=headers['id'])
            page.requested = False
            if data:
                if headers['last']:
                    page.last = False
                    p = Page(
                        server=page.server,
                        tier=page.tier,
                        division=page.division,
                        queue=page.queue,
                        page=page.page + 1
                    )
                    p.save()
                    Page.objects.filter(id=p.id).all().update(last_updated=timezone.now() - timedelta(days=1))

                self.update_user(data)
            page.save()
            channel.basic_ack(delivery_tag=method.delivery_tag)
        connection.close()

    def update_user(self, data):
        print("Adding/Updating user")
        user = []
        for entry in data:
            player = Player.objects.filter(
                server=settings.SERVER,
                summoner_id=entry['summonerId']).first()

            if not player:
                player = Player(
                    server=settings.SERVER,
                    summoner_id=entry['summonerId'])
            player.update(entry)
            player.save()

class Publisher(WorkerThread):

    def run(self):
        print("Started Sending worker.")
        # Establish connections and basics
        connection = pika.BlockingConnection(pika.ConnectionParameters(config['HOST']))
        queue = settings.SERVER + '_LEAGUE'
        print("Sending to " + queue)
        # Sending messages
        channel = connection.channel()
        while not self._is_interrupted:
            outdated = Page.objects.filter(
                server=settings.SERVER,
                last_updated__lte=timezone.now() - timedelta(hours=2)).all()[:10]
            if not outdated:
                print("Found no outdated")
                time.sleep(5)
                continue
            print("Found Outdated")
            for entry in outdated:
                message_body = {
                    "method": "entries",
                    "params": {
                        'queue': entry.queue,
                        'tier': entry.tier,
                        'division': entry.division,
                        'page': entry.page
                    }
                }
                headers = {
                    "id": entry.id,
                    "last": entry.last,
                    "return": queue + "_RET"
                }
                channel.basic_publish(
                    exchange="",
                    routing_key=queue,
                    body=json.dumps(message_body),
                    properties=pika.BasicProperties(
                        headers=headers
                    )
                )
                entry.requested = True
                entry.save()
        print("Received interrupt. Shutting down.")
        connection.close()


async def process_entries(message):

    data = json.loads(message.body.decode())
    print(f"Adding {len(data)} players to the db.")
    if len(data) == 0:
        return False
    for player in data:
        player_database = await asyncio.get_event_loop().run_in_executor(
            None,
            Player.objects.filter(
                server=settings.SERVER,
                summoner_id=player['summonerId']).first)

        if not player_database:
            player_database = Player(
                server=settings.SERVER,
                summoner_id=player['summonerId'])
        await player_database.update(player)
        await asyncio.get_event_loop().run_in_executor(
            None,
            player_database.save)

    return True

def parse_message(channel, method_frame, header_frame, body):

    print(method_frame.delivery_tag)
    print(header_frame)
    print(body)
    print()

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)


async def update_tier(counter, channel, queue_out, queue_return):

    has_content = True
    start_page = 1
    empties = 0
    while has_content:
        ids = []
        for i in range(start_page, counter.pages+1):
            message_body = {
                    "method": "entries",
                    "params": {
                        'queue': counter.queue,
                        'tier': counter.tier,
                        'division': counter.division,
                        'page': i
                    }
                }
            print(message_body)
            await channel.default_exchange.publish(

                aio_pika.Message(
                    headers={'return': settings.SERVER + "_league", 'id': str(i)},
                    body=json.dumps(message_body).encode()
                ),
                routing_key=settings.SERVER + '_LEAGUE_RET'
            )
            ids.append(str(i))
        print(ids)
        while ids:
            try:
                message = await queue_return.get(timeout=5)
                await message.ack()
                info = message.info()
                print(info)
                print(f"Waiting for {len(ids)} IDs.")
                ids.pop(ids.index(message.headers_raw['id'].decode()))
                if not await process_entries(message):
                    has_content = False
                    empties += 1
            except Exception as err:
                print(err)
                await asyncio.sleep(0.5)
        if has_content:
            start_page = counter.pages+1
            counter.pages += 10
        else:
            counter.pages -= empties
        await asyncio.get_event_loop().run_in_executor(
            None,
            counter.save)

def main():

    # Create initial pages if none exist:
    print(Page.objects.count())
    #Page.objects.all().delete()
    #exit()
    if Page.objects.count() == 0:
        for queue in queues:
            for tier in tiers:
                for division in divisions:
                    p = Page(
                        server=settings.SERVER,
                        queue=queue,
                        tier=tier,
                        division=division,
                        page=1,
                        last=True
                    )
                    p.save()
        Page.objects.all().update(last_updated=timezone.now() - timedelta(days=1))

    listener = Listener()
    listener.start()

    publisher  = Publisher()
    publisher.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        listener.stop()
        publisher.stop()
    listener.join()
    publisher.join()

if __name__ == "__main__":
    main()