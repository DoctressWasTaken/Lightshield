import time
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import threading
from tables import Base, Match
from tables.enums import Server


class Worker(threading.Thread):

    def __init__(self, tasks, echo=False):
        super().__init__()
        self.tasks = tasks
        self.engine = create_engine(
            f'postgresql://%s@%s:%s/data' %
            (os.environ['POSTGRES_USER'],
             os.environ['POSTGRES_HOST'],
             os.environ['POSTGRES_PORT']),
            echo=echo)

        Base.metadata.create_all(self.engine)
        self.server = os.environ['SERVER']
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def run(self):
        inserted = 0
        print("Starting Match Worker..")
        while True:
            match = self.get_task()
            self.process_task(match)
            if (inserted := inserted + 1) == 500:
                print("Inserting 500 Matches. Remaining tasks: {len(self.tasks)}.")
                self.session.commit()
                inserted = 0

    def get_task(self):
        while not self.tasks:
            time.sleep(0.5)
        return self.tasks.pop()

    def process_task(self, match):

        if False and not self.session.query(Match) \
                .filter_by(matchId=match['gameId']) \
                .filter_by(server=Server.get(self.server)) \
                .first():
            match = Match(
                matchId=match['gameId'],
                queue=match['queueId'],
                gameDuration=match['gameDuration'],
                server=Server.get(self.server),
                gameCreation=match['gameCreation'],
                seasonId=match['seasonId'],
                gameVersion=match['gameVersion'],
                mapId=match['mapId'],
                gameMode=match['gameMode'],
                participantIdentities=match['participantIdentities'],
                teams=match['teams'],
                participants=match['participants'])
            self.session.add(match)

        # self.session.commit()
