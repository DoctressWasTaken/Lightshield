import pika
import pickle
import os
import logging
import threading

class RabbitManager(threading.Thread):

    def __init__(self, incoming=None, exchange=None, outgoing=(), prefetch=50):
        super().__init__()
        self.logging = logging.getLogger("RabbitMQ")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [RabbitMQ] %(message)s'))
        self.logging.addHandler(handler)

        self.stopped = False
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch)

        self.tasks = []
        self.packages = []
        self.server = os.environ['SERVER']
        self.blocked = False
        self.max_buffer = int(os.environ['MAX_TASK_BUFFER'])

        self.incoming = None
        self.exchange = None
        self.outgoing = outgoing

        if incoming:
            self.incoming = self.server + "_" + incoming
            self.channel.queue_declare(
                queue=self.incoming,
                durable=True)
        if exchange:
            self.exchange = self.server + "_" + exchange
            self.channel.exchange_declare(
                exchange=self.exchange,
                durable=True,
                exchange_type='topic')

    def shutdown(self):
        self.stopped = True

    def run(self):
        while not self.stopped:
            self.process_outgoing()
            self.check_size()
            if self.incoming:
                self.process_incoming()
        self.process_outgoing()

    def check_size(self):
        prev_blocked = self.blocked
        self.blocked = False
        for queue in self.outgoing:
            res = self.channel.queue_declare(
                queue=self.server + "_" + queue,
                passive=True
            )
            if res.method.message_count > self.max_buffer:
                if not prev_blocked:
                    self.logging.info("Queue %s is too full [%s/%s]",
                                      queue, res.method.message_count, self.max_buffer)
                self.blocked = True
        if not self.blocked and prev_blocked:
            self.logging.info("Blocker released.")

    def process_incoming(self):
        while len(self.tasks) < 10:
            self.channel.basic_get(self.incoming)

    def process_outgoing(self):
        for element in self.packages:
            if "message" in element:
                self.channel.basic_ack(delivery_tag=element['message'][1].delivery_tag)

            body = pickle.dumps(element['body'])
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key="",
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2  # make message persistent
                )
            )
