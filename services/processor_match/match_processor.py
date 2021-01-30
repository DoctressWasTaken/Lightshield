import threading
import logging
import asyncio
import pickle
from lol_dto import Match
import traceback
import aio_pika
import asyncpg


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
                            tasks.append(items)

                        if len(tasks) >= 50 or self.stopped:
                            break

                match_list = []
                team_list = []
                player_list = []
                runes_list = []
                m_keys = [col.__str__().split(".")[0] for col in tasks[0]['match'].__table__.columns]
                t_keys = [col.__str__().split(".")[0] for col in tasks[0]['team'][0].__table__.columns]
                p_keys = [col.__str__().split(".")[0] for col in tasks[0]['player'][0].__table__.columns]
                r_keys = [col.__str__().split(".")[0] for col in tasks[0]['runes'][0][0].__table__.columns]

                for entry in tasks:
                    m = entry['match']
                    match_list.append(", ".join(await create_set([m.__dict__[key] for key in m_keys])))

                    for team in entry['team']:
                        team_list.append(", ".join(await create_set([team.__dict__[key] for key in t_keys])))

                    for index, player in enumerate(entry['player']):
                        player_list.append(", ".join(await create_set([player.__dict__[key] for key in p_keys])))
                        for i in range(6):
                            runes_list.append(
                                ", ".join(await create_set([entry['runes'][index][i].__dict__[key] for key in r_keys])))

                self.logging.info(match_list)
                self.logging.info(team_list)
                self.logging.info(player_list)
                self.logging.info(runes_list)

            except Exception as err:
                traceback.print_tb(err.__traceback__)
                print(err)
            await asyncio.sleep(15)
            return
            # tasks.append(pickle.loads(task.body))

            # task.ack()
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
        self.logging.info("Initiated Worker.")
        self.connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq/", loop=asyncio.get_running_loop()
        )
        await asyncio.gather(*[asyncio.create_task(self.async_worker()) for _ in range(1)])

    def shutdown(self):
        self.stopped = True
