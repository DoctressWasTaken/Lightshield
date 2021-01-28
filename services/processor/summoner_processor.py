import threading
import logging
import pika
import pickle
from permanent_db import PermanentDB


class SummonerProcessor(threading.Thread):

    def __init__(self, server, offset, patches, permanent):
        super().__init__()
        self.logging = logging.getLogger("SummonerProcessor")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [SummonerProcessor] %(message)s'))
        self.logging.addHandler(handler)

        self.stopped = False
        self.server = server
        self.offset = offset
        self.patches = patches
        self.permanent = permanent
        self.current_patch = self.patches[
            sorted(self.patches.keys(), reverse=False)[0]]['name']

        self.channel = None
        self.consumer = None

    def run(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('rabbitmq')
        )
        channel = connection.channel()
        channel.queue_declare(
            queue=self.server + '_SUMMONER_TO_PROCESSOR',
            durable=True)
        self.consumer = channel.basic_consume(
            queue=self.server + '_SUMMONER_TO_PROCESSOR',
            on_message_callback=self.on_message,
            auto_ack=False)
        self.channel = channel
        channel.start_consuming()

    def shutdown(self):
        self.stopped = True
        self.channel.basic_cancel(self.consumer)
        self.permanent.commit_db(self.current_patch)

    def update_patches(self, patches):
        self.patches = patches
        self.permanent.commit_db(self.current_patch)
        self.current_patch = self.patches[
            sorted(self.patches.keys(), reverse=False)[0]]['name']

    def on_message(self, ch, method, properties, body):
        accountId, puuid, rank, wins, losses = pickle.loads(body)

        self.permanent.add_summoner(self.current_patch, accountId, puuid, rank, wins, losses)

        ch.basic_ack(delivery_tag=method.delivery_tag)


