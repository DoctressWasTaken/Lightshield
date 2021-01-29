import threading
import logging
import asyncio
import pickle
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika
import traceback


class SummonerProcessor(threading.Thread):

    def __init__(self, server, db):
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
        self.db = db

    async def async_worker(self):
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=100)
        queue = await channel.declare_queue(
            name=self.server + "_SUMMONER_TO_PROCESSOR",
            durable=True,
            robust=True
        )
        while not self.stopped:
            tasks = []
            try:
                while len(tasks) < 50 and not self.stopped:
                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                tasks.append(pickle.loads(message.body))
                            if len(tasks) >= 50 or self.stopped:
                                break

                    if len(tasks) < 50 and not self.stopped:
                        await asyncio.sleep(2)
                self.logging.info("Inserting %s summoner.", len(tasks))
                value_lists = ["('%s', '%s', %s, %s, %s)" % tuple(task) for task in tasks]
                values = ",".join(value_lists)
                async with AsyncSession(self.db.engine) as session:
                    async with session.begin():
                        query = """
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
                        print(query)
                        await session.execute(query)
                    await session.commit()

            except Exception as err:
                traceback.print_tb(err.__traceback__)


    async def run(self):
        self.logging.info("Initiated Worker.")
        self.connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop()
        )
        await asyncio.gather(*[asyncio.create_task(self.async_worker()) for _ in range(5)])

    def shutdown(self):
        self.stopped = True
