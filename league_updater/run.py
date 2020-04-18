"""League updater - Run Module."""
import os
import django
from datetime import timedelta
from django.utils import timezone
import pika
import json
import threading
import time
config = json.loads(open('../config.json').read())

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "playerdata.settings")
django.setup()

from django.conf import settings
from data.models import Player, Page

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
        connection = pika.BlockingConnection(pika.ConnectionParameters(config['HOST']))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        listen_queue = settings.SERVER + "_LEAGUE-EXP_RET"
        print(f"Listening on {listen_queue}")
        q = channel.queue_declare(queue=listen_queue, durable=True)

        for message in channel.consume(listen_queue, inactivity_timeout=1):
            if self._is_interrupted:
                print("Received interrupt. Shutting down.")
                break
            if all(x is None for x in message):
                continue

            method, properties, body = message
            data = json.loads(body)

            headers = properties.headers
            print(headers)

            page = Page.objects.filter(
                server=settings.SERVER,
                tier=headers['tier'],
                division=headers['division'],
                queue=headers['queue'],
                page=headers['page']
            ).first()

            if not page:
                print("Page not found, removing message.")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                continue
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
        changed_user = []
        new_user = []
        for entry in data:
            player = Player.objects.filter(
                server=settings.SERVER,
                summoner_id=entry['summonerId']).first()

            if not player:
                player = Player(
                    server=settings.SERVER,
                    summoner_id=entry['summonerId'])
                player.update(entry)
                new_user.append(player)
                continue
            if player.update(entry):
                changed_user.append(player)
        Player.objects.bulk_create(new_user)
        Player.objects.bulk_update(changed_user, [
            'summoner_name',
            'ranking_solo',
            'ranking_flex',
            'series_solo',
            'series_flex',
            'wins_solo',
            'losses_solo',
            'wins_flex',
            'losses_flex',
            'update_solo',
            'update_flex'])

class Publisher(WorkerThread):

    def run(self):
        print("Started Sending worker.")
        # Establish connections and basics
        connection = pika.BlockingConnection(pika.ConnectionParameters(config['HOST']))
        queue = settings.SERVER + '_LEAGUE-EXP'
        print("Sending to " + queue)
        # Sending messages
        channel = connection.channel()
        while not self._is_interrupted:
            q = channel.queue_declare(queue=queue, durable=True)
            length = q.method.message_count
            if length > 50:
                print("Too many tickets, waiting.")
                time.sleep(10)
                continue
            outdated = Page.objects.filter(
                    server=settings.SERVER,
                    last_updated__lte=timezone.now() - timedelta(minutes=5),
                    last=True).all()[:10]
            if not outdated:
                time.sleep(5)
                continue
                outdated = Page.objects.filter(
                        server=settings.SERVER,
                        last_updated__lte=timezone.now() - timedelta(minutes=5))\
                    .order_by('last_updated').all()[:10]
            if not outdated:
                time.sleep(5)
                continue

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
                    'queue': entry.queue,
                    'tier': entry.tier,
                    'division': entry.division,
                    'page': entry.page,
                    "last": entry.last,
                    "return": queue + "_RET"
                }
                channel.basic_publish(
                    exchange="",
                    routing_key=queue,
                    body=json.dumps(message_body),
                    properties=pika.BasicProperties(
                        headers=headers,
                        delivery_mode=2
                    )
                )
                entry.requested = True
                entry.save()
        print("Received interrupt. Shutting down.")
        connection.close()

def main():

    # Create initial pages if none exist:
    if Page.objects.count() == 0:
        for queue in queues:
            for tier in tiers:
                if tier not in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
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
                else:
                    p = Page(
                        server=settings.SERVER,
                        queue=queue,
                        tier=tier,
                        division="I",
                        page=1,
                        last=True
                    )
                    p.save()
        Page.objects.all().update(last_updated=timezone.now() - timedelta(days=1))

    listeners = []
    for i in range(5):
        listener = Listener()
        listener.start()
        listeners.append(listener)

    publisher  = Publisher()
    publisher.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        for listener in listeners:
            listener.stop()
        publisher.stop()
    for listener in listeners:
        listener.join()
    publisher.join()


if __name__ == "__main__":
    main()