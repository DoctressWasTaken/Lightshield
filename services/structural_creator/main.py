import aio_pika
import asyncio
import os

from aio_pika import ExchangeType

server = os.environ['SERVER']

async def main(loop):
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@rabbitmq/", loop=loop)
    channel = await connection.channel()

    # League Rankings
    rankings_out = await channel.declare_exchange(
        name="%s_RANKED" % server,
        durable=True,
        type=ExchangeType.TOPIC)

    # Summoner IDs
    summoner_in = await channel.declare_queue(
        name='%s_RANKED_TO_SUMMONER' % server,
        durable=True
    )
    summoner_in.bind(rankings_out, routing_key="#")
    summoner_out = await channel.declare_exchange(
        name="%s_SUMMONER" % server,
        durable=True,
        type=ExchangeType.TOPIC)

    # Match History
    history_in = await channel.declare_queue(
        name='%s_SUMMONER_TO_HISTORY' % server,
        durable=True)
    history_in.bind(summoner_out, routing_key="#")
    history_out = await channel.declare_exchange(
        name="%s_HISTORY" % server,
        durable=True,
        type=ExchangeType.TOPIC)

    # Match Details
    details_in = await channel.declare_queue(
        name="%s_HISTORY_TO_DETAILS" % server,
        durable=True)
    details_in.bind(history_out, routing_key="#")
    details_out = await channel.declare_exchange(
        name="%s_DETAILS" % server,
        durable=True,
        type=ExchangeType.TOPIC)

    # Processor
    processor_summoner_in = await channel.declare_queue(
        name="%s_SUMMONER_TO_PROCESSOR" % server,
        durable=True
    )
    processor_summoner_in.bind(summoner_out, routing_key="#")
    processor_details_in = await channel.declare_queue(
        name="%s_DETAILS_TO_PROCESSOR" % server,
        durable=True)
    processor_details_in.bind(details_out, routing_key="#")



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()