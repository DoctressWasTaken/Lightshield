import threading
import logging
import asyncio
import pickle
from rabbit_manager import RabbitManager
from sqlalchemy.ext.asyncio import AsyncSession
from lol_dto import Match
import traceback

class MatchProcessor(threading.Thread):

    def __init__(self, server, permanent):
        super().__init__()
        self.logging = logging.getLogger("MatchProcessor")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [MatchProcessor] %(message)s'))
        self.logging.addHandler(handler)
        self.logging.info("Initiated match Processor.")

        self.stopped = False
        self.server = server
        self.permanent = permanent

        self.rabbit = RabbitManager(
            exchange='temp',
            incoming="DETAILS_TO_PROCESSOR"
        )

    async def async_worker(self):
        self.logging.info("Initiated Worker.")
        while not self.stopped:
            tasks = []
            while len(tasks) < 50 and not self.stopped:
                await asyncio.sleep(1)
                self.logging.info("Getting task")
                self.logging.info(self.rabbit.queue.qsize())
                try:
                    task = self.rabbit.queue.get_nowait()
                except Exception as err:
                    self.logging.info(err)
                    await asyncio.sleep(1)
                    self.logging.info("No task found")
                    continue
                self.logging.info(self.rabbit.queue.qsize())
                try:
                    message = pickle.loads(task.body)
                    self.logging.info(message)
                    items = await Match.create(message)
                    self.logging(items)
                    self.logging(items['match'].__dict__)
                    self.logging(list(items['match']))
                except Exception as err:
                    traceback.print_tb(err.__traceback__)
                    print(err)
                await asyncio.sleep(15)
                return
                #tasks.append(pickle.loads(task.body))

                #task.ack()
            if len(tasks) == 0 and self.stopped:
                return
            self.logging.info("Inserting %s summoner.", len(tasks))
            value_lists = ["('%s', '%s', %s, %s, %s)" % tuple(task) for task in tasks]
            values = ",".join(value_lists)
            async with AsyncSession(await self.permanent.get_engine()) as session:
                async with session.begin():
                    await session.execute(
                        """
                        INSERT INTO summoner 
                        (accountId, puuid, rank, wins, losses)
                        VALUES %s
                        ON COMFLICT (puuid)
                        DO
                            UPDATE SET rank = EXCLUDED.rank,
                                       wins = EXCLUDED.wins,
                                       losses = EXCLUDED.losses
                        ;
                        """ % values
                    )
                await session.commit()

    async def run(self):
        await self.rabbit.init()
        fill_task = asyncio.create_task(self.rabbit.fill_queue())
        await asyncio.gather(*[asyncio.create_task(self.async_worker()) for _ in range(1)])
        await fill_task


    def shutdown(self):
        self.stopped = True
