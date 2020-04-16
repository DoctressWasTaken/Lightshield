#!/usr/bin/env python

import asyncio
import aio_pika
import json
config = json.loads(open('../config.json').read())

from api import API


server_ids = ['EUW1', 'KR1', 'NA1']


async def main(loop):
    target = "amqp://guest:guest@" + config['HOST']
    
    while True:
        try:
            connection = await aio_pika.connect_robust(
            target, loop=loop
            )
            break
        except:
            print("RabbitMQ not ready yet")
            await asyncio.sleep(1)
            continue

    apis = []
    for server in server_ids:
        api = API(server, config['API_KEY'])
        apis.append(asyncio.create_task(api.run(connection)))
    await asyncio.gather(*apis)

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
