import asyncio
import aio_pika


async def main(loop):
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@localhost/", loop=loop)

    async with connection:
        routing_key = "requests"

        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue(
            routing_key
        )
        while True:
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body='Hello World'.encode()
                ),
                routing_key=routing_key
            )
            await asyncio.sleep(3)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()