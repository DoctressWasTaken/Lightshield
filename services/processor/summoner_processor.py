import threading
import logging
import asyncio
import pickle
from permanent_db import PermanentDB
from rabbit_manager import RabbitManager
from sqlalchemy.ext.asyncio import AsyncSession

class SummonerProcessor(threading.Thread):

    def __init__(self, server, offset, patches, permanent):
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
        self.offset = offset
        self.patches = patches
        self.permanent = permanent
        self.current_patch = self.patches[
            sorted(self.patches.keys(), reverse=False)[0]]['name']

        self.channel = None
        self.consumer = None

        self.rabbit = RabbitManager(
            incoming="SUMMONER_TO_PROCESSOR"
        )
    async def async_worker(self):
        while not self.stopped:
            tasks = []
            while len(tasks) < 50 and not self.stopped:
                if not (task := await self.rabbit.get()):
                    await asyncio.sleep(1)
                    continue

                tasks.append(pickle.loads(task.body))
                task.ack()
            if len(tasks) ==0 and self.stopped:
                return
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
        await asyncio.gather([asyncio.create_task(self.async_worker()) for _ in range(5)])

    def shutdown(self):
        self.stopped = True
        self.channel.basic_cancel(self.consumer)
        self.permanent.commit_db(self.current_patch)

    async def update_patches(self, patches):
        self.patches = patches
        self.permanent.commit_db(self.current_patch)
        self.current_patch = self.patches[
            sorted(self.patches.keys(), reverse=False)[0]]['name']

    def on_message(self, ch, method, properties, body):
        accountId, puuid, rank, wins, losses = pickle.loads(body)

        self.permanent.add_summoner(self.current_patch, accountId, puuid, rank, wins, losses)

        ch.basic_ack(delivery_tag=method.delivery_tag)


