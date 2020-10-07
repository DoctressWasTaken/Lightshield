import time
import os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import threading
from tables import Base, Match
from tables.enums import Server
import logging
import asyncio
import aioredis


class Service(threading.Thread):

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
        self.session = self.Session()
        self.stopped = False

        self.logging = logging.getLogger("DBConnector")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [DBConnector] %(message)s'))
        self.logging.addHandler(handler)

        self.redisc = None

    def shutdown(self):
        """Set stopping flag.

        Handler called by the sigterm signal.
        """
        self.logging.info("Received shutdown signal.")
        self.stopped = True

    async def init(self):
        self.redisc = await aioredis.create_redis_pool(
            ('redis', 6379), db=0, encoding='utf-8')

    async def get_tasks(self):
        while (length := await self.redisc.llen('tasks')) < 10 and not self.stopped:
            await asyncio.sleep(0.5)
        if self.stopped:
            return

        tasks = []
        while task := await self.redisc.lpop('tasks'):
            tasks.append(task)
        return tasks

    def run(self):

        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.init())
        self.logging.info("Starting Match Worker..")
        while not self.stopped:
            tasks = loop.run_until_complete(self.get_tasks())
            if not tasks:
                continue
            db_objects = self.process_tasks(tasks)
            self.logging.info("Inserting %s Matches.", len(db_objects))
            self.session.bulk_save_objects(db_objects)
            self.session.commit()
        self.logging.info("Shutting down DB_Connector")

    def process_tasks(self, tasks):
        db_objects = []
        ids = []
        while tasks:
            match = json.loads(tasks.pop())
            if not self.session.query(Match) \
                    .filter_by(matchId=match['gameId']) \
                    .filter_by(server=Server.get(self.server)) \
                    .first():
                if match['gameId'] in ids:
                    continue
                ids.append(match['gameId'])
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
                db_objects.append(match)
        return db_objects
