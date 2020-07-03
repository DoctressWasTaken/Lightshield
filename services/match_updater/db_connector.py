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
        print("Starting Match Worker..")
        while True:
            while len(self.tasks) < 100:
                time.sleep(0.5)
            task_list = self.process_task()
            print(f"Inserting {len(task_list)} Matches. Remaining tasks: {len(self.tasks)}.")
            self.session.bulk_save_objects(task_list)
            self.session.commit()

    def get_task(self):
        while not self.tasks:
            time.sleep(0.5)
        return self.tasks.pop()

    def process_task(self):
        tasks_list = []
        while self.tasks:
            match = self.tasks.pop()
            if not self.session.query(Match) \
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
                tasks_list.append(match)
        return tasks_list
        # self.session.commit()
