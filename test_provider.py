import asyncio
import aio_pika
import json
import random

async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        info = message.info()
        print(message.headers_raw)
        print(message.body)

async def main(loop):
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@localhost/", loop=loop)

    routing_key = "requests"

    channel = await connection.channel()

    # Declaring queue
    queue = await channel.declare_queue(
        routing_key
    )
    response = await channel.declare_queue(
        'league'
    )
    ids = []

    for i in range(20):
        data = json.dumps([
            'EUW1',
            'league',
            'entries',
            {
                'queue': 'RANKED_SOLO_5x5',
                'tier': 'DIAMOND',
                'division': 'I',
                'page': i+1}
        ])
        ids.append(str(i))
        await channel.default_exchange.publish(

            aio_pika.Message(
                headers={'out': 'league', 'id': str(i)},
                body=data.encode()
            ),
            routing_key=routing_key
        )
    await asyncio.sleep(2)
    while ids:
        message = await response.get(timeout=5)
        await message.ack()
        info = message.info()
        print(message.headers_raw)
        print(message.body)
        ids.pop(ids.index(message.headers_raw['id'].decode()))
        print(ids)
    try:
        while True:
            message = await response.get(timeout=5)
            await message.ack()
    except:
        pass
    await connection.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))