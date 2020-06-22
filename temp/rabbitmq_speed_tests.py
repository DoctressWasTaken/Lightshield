import aio_pika
import asyncio
import datetime
from aio_pika import Message
from aio_pika.pool import Pool

from pika_connector import Pika

async def main():

    pika = Pika(host="localhost")
    await pika.init()
    results = []
    for i in range(10):
        print("-" * 30)
        start = datetime.datetime.now()
        print(start)
        loop = asyncio.get_event_loop()
        for i in range(10000):
            await pika.push("Hello World")
        end = datetime.datetime.now()
        print(end)
        print((end - start).total_seconds())
        results.append((end - start).total_seconds())
        print(10000 / (end - start).total_seconds(), "per second.")
    if not True:
        async def get_connection():
            return await aio_pika.connect_robust("amqp://guest:guest@localhost/")

        connection_pool = Pool(get_connection, max_size=4, loop=loop)

        async def get_channel() -> aio_pika.Channel:
            async with connection_pool.acquire() as connection:
                return await connection.channel()

        channel_pool = Pool(get_channel, max_size=10, loop=loop)
        queue_name = "pool_queue"
        exceptions = 0
        async def publish():
            global exceptions
            try:
                async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
                    await channel.default_exchange.publish(
                        aio_pika.Message(("Channel: %r" % channel).encode()),
                        queue_name,
                    )
            except:
                exceptions += 1

        async with connection_pool, channel_pool:
            await asyncio.wait([publish() for _ in range(10000)])
        print("Exceptions:", exceptions)
        end = datetime.datetime.now()
        print(end)
        print((end - start).total_seconds())
        results.append((end - start).total_seconds())
        print(10000/(end - start).total_seconds(), "per second.")
    print(results)


async def main2():

    start = datetime.datetime.now()
    print(start)
    async with await aio_pika.connect('amqp://guest:guest@localhost/') as connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue("test_queue", durable=True)

        for i in range(10000):
            await channel.default_exchange.publish(
                Message(bytes('Hello_World', 'utf-8')),
                routing_key="test_queue")
    end = datetime.datetime.now()
    print(end)
    print((end - start).total_seconds())
    print(10000/(end - start).total_seconds(), "per second.")

    results = []
    for i in range(10):
        print("-" * 30)
        start = datetime.datetime.now()
        print(start)
        loop = asyncio.get_event_loop()

        async def get_connection():
            return await aio_pika.connect_robust("amqp://guest:guest@localhost/")

        connection_pool = Pool(get_connection, max_size=4, loop=loop)

        async def get_channel() -> aio_pika.Channel:
            async with connection_pool.acquire() as connection:
                return await connection.channel()

        channel_pool = Pool(get_channel, max_size=10, loop=loop)
        queue_name = "pool_queue"
        exceptions = 0
        async def publish():
            global exceptions
            try:
                async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
                    await channel.default_exchange.publish(
                        aio_pika.Message(("Channel: %r" % channel).encode()),
                        queue_name,
                    )
            except:
                exceptions += 1

        async with connection_pool, channel_pool:
            await asyncio.wait([publish() for _ in range(10000)])
        print("Exceptions:", exceptions)
        end = datetime.datetime.now()
        print(end)
        print((end - start).total_seconds())
        results.append((end - start).total_seconds())
        print(10000/(end - start).total_seconds(), "per second.")
    print(results)

asyncio.run(main())