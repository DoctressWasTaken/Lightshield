import pika
import json
import time
import os
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Enum
from sqlalchemy.orm import sessionmaker
import threading
from tables import Base, Match
from tables.enums import Server

class Worker(threading.Thread):

    def __init__(self, echo=False):
        super().__init__()
        self.engine = create_engine(
                f'postgresql://%s@%s:%s/data' %
                (os.environ['POSTGRES_USER'],
                 os.environ['POSTGRES_HOST'],
                 os.environ['POSTGRES_PORT']),
                echo=echo)

        Base.metadata.create_all(self.engine)
        self.server = os.environ['SERVER']
        self.Session = sessionmaker(bind=self.engine)
        self.channel = None
        self.session = self.Session()

    def get_message(self):
        message = self.channel.basic_get(queue=f'DB_MATCH_IN_{self.server}')
        if all(x is None for x in message):
            return None
        return message

    def run(self):
        with pika.BlockingConnection(
                pika.ConnectionParameters(
                    'rabbitmq',
                    connection_attempts=2,
                    socket_timeout=30)) as rabbit_connection:
            self.channel = rabbit_connection.channel()
            inserted = 0
            print("Starting Match Worker..")
            while True:
                message = self.get_task()
                self.process_task(message)
                if (inserted := inserted + 1) == 500:
                    
                    print("Inserting 500 Matches.")
                    inserted = 0

    def get_task(self):
        while not (message := self.get_message()):
            time.sleep(0.5)
        return message

    def process_task(self, message):

        match = json.loads(message[2])
        """
        if False and not self.session.query(Match)\
            .filter_by(matchId=match['gameId'])\
            .filter_by(server=Server.get(self.server))\
                        .first():
            match = Match(
                matchId = match['gameId'],
                queue=match['queueId'],
                gameDuration=match['gameDuration'],
                server=Server.get(self.server),
                gameCreation=match['gameCreation'],
                seasonId=match['seasonId'],
                gameVersion=match['gameVersion'],
                mapId=match['mapId'],
                gameMode=match['gameMode'])
       """ 
            
            #self.session.add(match)
            #self.session.commit()
        self.channel.basic_ack(
                delivery_tag=message[0].delivery_tag)


if __name__ == "__main__":
    worker = Worker()
    worker.run()
