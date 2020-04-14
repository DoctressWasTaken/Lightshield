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

        await channel.default_exchange.publish(

            aio_pika.Message(
                headers={'out': 'league', 'id': str(i)},
                body=data.encode()
            ),
            routing_key=routing_key
        )

    await response.consume(callback=process_message)
    return connection

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    connection = loop.run_until_complete(main(loop))

    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(connection.close())