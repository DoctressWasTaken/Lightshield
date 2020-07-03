"""Summoner inserting worker class. Launched as a thread."""
import json
import time
import os
import threading
import pika
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tables import Base, Summoner
from tables.enums import Tier, Rank, Server


class Worker(threading.Thread):
    """Worker Thread."""
    def __init__(self, logging, echo=False):
        """Initiate SQLAlchemy engine and settings.

        :param logger: Default Python Logger.
        :param echo: Boolean to show output of SQLAlchemy.
        """
        super().__init__()
        self.engine = create_engine(
            'postgresql://%s@%s:%s/data' %
            (os.environ['POSTGRES_USER'],
             os.environ['POSTGRES_HOST'],
             os.environ['POSTGRES_PORT']),
            echo=echo)

        Base.metadata.create_all(self.engine)
        self.server = os.environ['SERVER']
        self.Session = sessionmaker(bind=self.engine)  # pylint: disable=C0103
        self.channel = None
        self.session = self.Session()
        self.logging = logging

    def get_message(self):
        """Await message from rabbitmq.

        :return RabbitMQ message or None if none is found.
        """
        message = self.channel.basic_get(queue=f'DB_SUMMONER_IN_{self.server}')
        if all(x is None for x in message):
            return None
        return message

    def run(self):
        """Start core worker loop."""
        with pika.BlockingConnection(
                pika.ConnectionParameters(
                    'rabbitmq',
                    connection_attempts=2,
                    socket_timeout=30)) as rabbit_connection:
            self.channel = rabbit_connection.channel()
            inserted = 0
            self.logging.info("Starting Summoner Worker.")
            while True:
                message = self.get_task()
                self.process_task(message)
                if (inserted := inserted + 1) == 200:
                    self.session.commit()
                    self.logging.info("Inserted 100 Summoner.")
                    inserted = 0

    def get_task(self):
        """Loop until a task is received then return it."""
        while not (message := self.get_message()):  # pylint: disable=C0325
            time.sleep(0.5)
        return message

    def process_task(self, message):
        """Process a received message and add it to the task list to be submitted to the db."""
        user = json.loads(message[2])
        if not self.session.query(Summoner).filter_by(puuid=user['puuid']).first():

            series = None
            if "miniSeries" in user:
                series = user['miniSeries']['progress'][:-1]

            summoner = Summoner(
                summonerId=user['summonerId'],
                accountId=user['accountId'],
                puuid=user['puuid'],
                summonerName=user['summonerName'],
                tier=Tier.get(user['tier']),
                rank=Rank.get(user['rank']),
                series=series,
                server=Server.get(self.server),
                wins=user['wins'],
                losses=user['losses'])
            self.session.add(summoner)
        self.channel.basic_ack(
            delivery_tag=message[0].delivery_tag)
