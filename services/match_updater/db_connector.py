import time
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import threading
from tables import Base, Match
from tables.enums import Server


class Worker(threading.Thread):

    def __init__(self, service, echo=False):
        super().__init__()
        self.service = service
        self.engine = create_engine(
            f'postgresql://%s@%s:%s/data' %
            (os.environ['POSTGRES_USER'],
             os.environ['POSTGRES_HOST'],
             os.environ['POSTGRES_PORT']),
            echo=echo)

        Base.metadata.create_all(self.engine)
        self.server = self.service.server
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def run(self):
        self.service.logging.info("Starting Match Worker..")
        while not self.service.stopping:
            while len(self.service.task_holder) < 100:
                time.sleep(0.5)
            task_list = self.process_task()
            self.service.logging.info(f"Inserting {len(task_list)} Matches. Remaining tasks: {len(self.service.task_holder)}.")
            self.session.bulk_save_objects(task_list)
            self.session.commit()
        time.sleep(2)
        if self.service.task_holder:
            task_list = self.process_task()
            self.service.logging.info(f"Inserting {len(task_list)} Matches. Remaining tasks: {len(self.service.task_holder)}.")
            self.session.bulk_save_objects(task_list)
            self.session.commit()

        self.service.logging.info("Shutting down DB_Connector")

    def get_task(self):
        while not self.service.task_holder and not self.service.stopping:
            if self.service.stopping:
                return None
            time.sleep(0.5)
        return self.service.task_holder.pop()

    def process_task(self):
        tasks_list = []
        while self.service.task_holder:
            match = self.service.task_holder.pop()
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
