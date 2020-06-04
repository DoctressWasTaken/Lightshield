import asyncio
import os
import websockets
import logging
import json


if "SERVER" not in os.environ:
    print("No server provided, shutting down")
    exit()


from redis_connector import Redis
from pika_connector import Pika

class Worker:

    def __init__(self):

        self.redis = Redis()
        self.pika = Pika()
        self.url_template = "/summoner/v4/summoners/%s"

        self.active_tasks = {}

        self.rejected = False

        self.logging = logging.getLogger("worker")
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [WORKER] %(message)s'))
        self.logging.addHandler(ch)

    async def send_tasks(self, tasks):

        call_tasks = [[summonerId, summonerId] for summonerId in tasks]

        message = {
            "endpoint": "summoner",
            "url": self.url_template,
            "tasks": call_tasks
        }
        await self.websocket.send(json.dumps(message))

    async def add_tasks(self):
        while True:
            await asyncio.sleep(1)
            if self.rejected:
                await asyncio.sleep(4)
                self.rejected = False
            messages_to_send = []
            if len(self.active_tasks) > 750:
                continue
            while len(self.active_tasks) < 800:
                msg = await self.pika.get()
                if not msg:
                    await asyncio.sleep(5)
                content = json.loads(msg.body.decode('utf-8'))
                summonerId = content['summonerId']

                if summonerId in self.active_tasks:
                    self.logging.info(f"Summoner id {summonerId} is already registered in active tasks.")
                    await msg.ack()
                    continue

                redis_entry = await self.redis.hgetall(summonerId)

                if redis_entry:
                    package = {**content, **redis_entry}
                    await self.pika.push(package)
                    await msg.ack()
                    continue

                self.active_tasks[summonerId] = msg
                messages_to_send.append(summonerId)
            if messages_to_send:
                self.logging.info(f"Adding {len(messages_to_send)} tasks.")
                await self.send_tasks(messages_to_send)


    async def process_tasks(self):
        while True:
            msg = await self.websocket.recv()
            data = json.loads(msg)
            self.logging.info(f"Received {len(data['tasks'])} tasks. ({data['status']})")
            if data['status'] == "rejected":
                for task in data['tasks']:
                    summonerId = task['id']
                    await self.active_tasks[summonerId].reject(requeue=True)
                    del self.active_tasks[summonerId]

            elif data['status'] == 'done':
                for task in data['tasks']:
                    summonerId = task['id']
                    if task['status'] == 200:
                        data = task['result']
                        await self.redis.hset(
                            summonerId=summonerId,
                            mapping={'puuid': data['puuid'],
                                     'accountId': data['accountId']})
                    elif task['status'] == 429:
                        await self.active_tasks[summonerId].reject(requeue=True)
                        del self.active_tasks[summonerId]
                    else:
                        await self.active_tasks[summonerId].ack()
                        del self.active_tasks[summonerId]

    async def run(self):
        await self.pika.init()
        self.websocket = await websockets.connect("ws://proxy:6789")
        await self.websocket.send("Summoner_Id_Updater")
        await asyncio.gather(
            asyncio.create_task(self.add_tasks()),
            asyncio.create_task(self.process_tasks()))



if __name__ == "__main__":

    worker = Worker()
    asyncio.run(worker.run())