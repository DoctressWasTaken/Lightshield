import os
import django
import asyncio
import aio_pika
import json
config = json.loads(open('../config.json').read())

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "league_updater.settings")
django.setup()

from django.conf import settings
from data.models import Player, Pagecount

tiers = [
    "IRON",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "DIAMOND"
]
divisions = [
    "I",
    "II",
    "III",
    "IV"
]
queues = [
    "RANKED_SOLO_5x5",
    "RANKED_FLEX_SR"
]

async def process_entries(message):

    data = json.loads(message.body.decode())
    print(f"Adding {len(data)} players to the db.")
    if len(data) == 0:
        return False
    for player in data:
        player_database = await asyncio.get_event_loop().run_in_executor(
            None,
            Player.objects.filter(
                server=settings.SERVER,
                summoner_id=player['summonerId']).first)

        if not player_database:
            player_database = Player(
                server=settings.SERVER,
                summoner_id=player['summonerId'])
        await player_database.update(player)
        await asyncio.get_event_loop().run_in_executor(
            None,
            player_database.save)

    return True


async def update_tier(counter, channel, queue_out, queue_return):

    has_content = True
    start_page = 1
    empties = 0
    while has_content:
        ids = []
        for i in range(start_page, counter.pages+1):
            message_body = {
                    "method": "entries",
                    "params": {
                        'queue': counter.queue,
                        'tier': counter.tier,
                        'division': counter.division,
                        'page': i
                    }
                }
            print(message_body)
            await channel.default_exchange.publish(

                aio_pika.Message(
                    headers={'return': settings.SERVER + "_league", 'id': str(i)},
                    body=json.dumps(message_body).encode()
                ),
                routing_key=settings.SERVER + '_LEAGUE'
            )
            ids.append(str(i))
        print(ids)
        while ids:
            try:
                message = await queue_return.get(timeout=5)
                await message.ack()
                info = message.info()
                print(info)
                print(f"Waiting for {len(ids)} IDs.")
                ids.pop(ids.index(message.headers_raw['id'].decode()))
                if not await process_entries(message):
                    has_content = False
                    empties += 1
            except Exception as err:
                print(err)
                await asyncio.sleep(0.5)
        if has_content:
            start_page = counter.pages+1
            counter.pages += 10
        else:
            counter.pages -= empties
        await asyncio.get_event_loop().run_in_executor(
            None,
            counter.save)

async def main(loop):

    # Establish connections and basics
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@"+ config['HOST'], loop=loop)

    outgoing = "requests"
    returning = settings.SERVER + "_league"
    channel = await connection.channel()
    queue_out = await channel.declare_queue(outgoing)
    queue_return = await channel.declare_queue(returning)

    #  Cleanup remaining requests in the queue in case the service crashed mid execution
    await queue_return.purge()

    for queue in queues:
        for tier in tiers:
            for division in divisions:
                counter = await asyncio.get_event_loop().run_in_executor(
                    None,
                    Pagecount.objects.filter(
                        queue=queue,
                        tier=tier,
                        division=division
                    ).first)
                if not counter:
                    counter = Pagecount(
                        queue=queue,
                        tier=tier,
                        division=division
                    )
                    await asyncio.get_event_loop().run_in_executor(
                        None,
                        counter.save)
                await update_tier(counter, channel, queue_out, queue_return)
                await queue_return.purge()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))