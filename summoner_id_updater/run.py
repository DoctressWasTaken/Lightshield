import os
import django
import pika
import json
from datetime import timedelta
from django.utils import timezone

import threading
import time

config = json.loads(open('../config.json').read())

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "playerdata.settings")
django.setup()

from django.conf import settings
from data.models import Player




class WorkerThread(threading.Thread):
    def __init__(self):
        super(WorkerThread, self).__init__()
        self._is_interrupted = False

    def stop(self):
        self._is_interrupted = True

    def run(self):
        pass


class Listener(WorkerThread):
    player_bulk = []

    def run(self):
        print("Started Listening Worker.")
        connection = pika.BlockingConnection(pika.ConnectionParameters(config['HOST']))
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)
        listen_queue = settings.SERVER + "_SUMMONER_RET"
        print(f"Listening on {listen_queue}")
        channel.queue_declare(queue=listen_queue)

        for message in channel.consume(listen_queue, inactivity_timeout=1):
            if self._is_interrupted:
                print("Received interrupt. Shutting down.")
                break
            if all(x is None for x in message):
                continue

            method, properties, body = message
            data = json.loads(body)

            headers = properties.headers
            player = Player.objects.get(id=headers['id'])
            player.account_id = data['accountId']
            player.puuid = data['puuid']
            self.player_bulk.append(player)
            if len(self.player_bulk) >= 200:
                Player.objects.bulk_update(self.player_bulk, ['account_id', 'puuid'])
                self.player_bulk = []
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
        queue = settings.SERVER + '_SUMMONER'
        print("Sending to " + queue)
        # Sending messages
        channel = connection.channel()
        while not self._is_interrupted:
            time.sleep(10)
            Player.objects\
                .filter(requested_ids__lte=timezone.now() - timedelta(minutes=5))\
                .all()\
                .update(requested_ids=None)

            no_id_player = Player.objects.filter(
                server=settings.SERVER,
                account_id__isnull=True,
                requested_ids__isnull=True).all()[:200]
            if not no_id_player:
                print("Found no player that require updating.")
                time.sleep(5)
                continue
            for entry in no_id_player:
                message_body = {
                    "method": "summonerId",
                    "params": {
                        'summonerId': entry.summoner_id
                    }
                }
                headers = {
                    "id": entry.id,
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



def main():
    listener = Listener()
    listener.start()

    publisher = Publisher()
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