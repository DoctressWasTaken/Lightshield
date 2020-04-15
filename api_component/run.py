#!/usr/bin/env python

import asyncio
import aio_pika
import json
config = json.loads(open('../config.json').read())

from api_component.api import API

server_ids = ['EUW1', 'NA1']
apis = {}
for entry in server_ids:
    apis[entry] = API(entry, config['API_KEY'])

connection = None

async def process_message(message: aio_pika.IncomingMessage):
    global api
    async with message.process():
        info = message.info()
        out = info['headers']['out'].decode()
        id = info['headers']['id'].decode()
        channel = await connection.channel()
        out_queue = await channel.declare_queue(
            out
        )
        data = json.loads(message.body)
        server = data.pop(0)
        print(server)
        for i in range(10):
            try:
                res = await apis[server].request(*data)
                data = json.dumps(res)
                await channel.default_exchange.publish(

                    aio_pika.Message(
                        headers={'id': id},
                        body=data.encode()
                    ),
                    routing_key=out
                )
                break
            except Exception as err:
                await asyncio.sleep(max(1, 0.1*pow(i+1, 2)))
                continue


async def main(loop):
    global connection

    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@localhost/", loop=loop
    )

    queue_name = "requests"
    # Creating channel
    channel = await connection.channel()

    # Declaring queue
    queue = await channel.declare_queue(
        queue_name
    )

    # Maximum message count which will be
    # processing at the same time.
    await channel.set_qos(prefetch_count=100)

    await queue.consume(callback=process_message)
    return connection

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    connection = loop.run_until_complete(main(loop))

    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(connection.close())