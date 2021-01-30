import threading
import logging
import asyncio
import pickle
from lol_dto import Match
import traceback
import aio_pika
import asyncpg
from sqlalchemy.ext.asyncio import AsyncSession

async def create_set(data_list):
    formatted_list = []
    for entry in data_list:
        if type(entry) == str:
            formatted_list.append("'%s'" % entry)
        else:
            formatted_list.append(entry)


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

    async def async_worker(self):
        self.logging.info("Initiated Worker.")
        connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop()
        )
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=100)
        queue = await channel.declare_queue(
            name=self.server + "_DETAILS_TO_PROCESSOR",
            durable=True,
            robust=True
        )

        while not self.stopped:
            tasks = []
            try:
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            task = pickle.loads(message.body)
                            items = await Match.create(task)
                            tasks += items

                        if len(tasks) >= 50 or self.stopped:
                            break
                if len(tasks) == 0 and self.stopped:
                    return
                async with AsyncSession(self.permanent.engine) as session:
                    async with session.begin():
                        session.bulk_save_objects(
                            tasks
                        )
                    await session.commit()

            except Exception as err:
                traceback.print_tb(err.__traceback__)
                self.logging.info(err)
            # tasks.append(pickle.loads(task.body))

            # task.ack()


    async def run(self):
        self.logging.info("Initiated Worker.")
        self.connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop()
        )
        await self.permanent.init()
        await asyncio.gather(*[asyncio.create_task(self.async_worker()) for _ in range(1)])

    def shutdown(self):
        self.stopped = True
