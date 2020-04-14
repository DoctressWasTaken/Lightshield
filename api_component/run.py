#!/usr/bin/env python

import asyncio
import aiohttp
import aio_pika
import json
config = json.loads(open('../config.json').read())


async def main(loop):
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@localhost/", loop=loop
    )
    queue_name = "requests"
    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue(
            queue_name
        )

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    print(message.body)

                    if queue.name in message.body.decode():
                        break

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()