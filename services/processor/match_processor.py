import threading
import logging
from permanent_db import PermanentDB
import pika
import pickle


class MatchProcessor(threading.Thread):

    def __init__(self, server, offset, patches, permanent):
        super().__init__()
        self.logging = logging.getLogger("MatchProcessor")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [MatchProcessor] %(message)s'))
        self.logging.addHandler(handler)

        self.stopped = False
        self.server = server
        self.offset = offset
        self.patches = patches
        self.permanent = permanent
        self.current_patch = sorted(self.patches.keys(), reverse=False)[0]

        self.channel = None
        self.consumer = None

    def run(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('rabbitmq')
        )
        channel = connection.channel()
        channel.queue_declare(
            queue=self.server + '_DETAILS_TO_PROCESSOR',
            durable=True)
        self.consumer = channel.basic_consume(
            queue=self.server + '_DETAILS_TO_PROCESSOR',
            on_message_callback=self.on_message,
            auto_ack=False)
        self.channel = channel
        channel.start_consuming()

    def shutdown(self):
        self.stopped = True
        self.channel.basic_cancel(self.consumer)

    def update_patches(self, patches):
        if patches.keys() != self.patches.keys():
            self.logging.info("Updating patches")
            self.patches = patches

    def on_message(self, ch, method, properties, body):
        match_details = pickle.loads(body)
        matchTime = match_details['gameCreation'] // 1000
        if sorted(self.patches)[0] + self.offset < matchTime:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        for limit in sorted(self.patches):
            if (limit + self.offset) > matchTime:
                self.permanent.add_match(
                    self.patches[limit]['name'], match_details)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

        self.logging.info("ReQing match.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
