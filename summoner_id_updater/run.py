"""Summoner ID Updater Module."""

import os
from ratelimit import limits, RateLimitException
import aiohttp
import json
import asyncio
import time
import redis
import pika
from aiohttp.client_exceptions import ClientConnectorError

if "SERVER" not in os.environ:
    print("No server provided, shutting down")
    exit()
server = os.environ['SERVER']

url_template = f"http://proxy:8000/summoner/v4/summoners/%s"


@limits(calls=250, period=10)
async def fetch(url, session):
    """Call method."""
    async with session.get(url) as response:
        resp = await response.json(content_type=None)
        if response.status == 429:
            print(response.headers)
        return response.status, resp, response.headers


async def to_fetch(url, session, element):
    """Call placeholder method.

    Added to allow catching the RateLimitException
    without breaking the asyncio.gather.
    """
    try:
        response = await fetch(url, session)
        return response + (element,)
    except RateLimitException:
        return 998, [], {}, element


async def main(data_list):
    """Call data collectively.

    Function that bundles all API calls.
    Returns once all calls are finished.
    """
    results = []
    async with aiohttp.ClientSession() as session:
        while data_list:
            forced_timeout = 0
            tasks = []
            tasks.append(asyncio.sleep(10))
            print("Starting call cycle")
            for i in range(min(250, len(data_list))):
                element = data_list.pop()
                tasks.append(
                    asyncio.create_task(
                        to_fetch(
                            url_template % (element[2]['summonerId']),
                            session,
                            element
                            )
                        )
                    )
                await asyncio.sleep(0.01)
            responses = await asyncio.gather(*tasks)
            responses.pop(0)
            for response in responses:
                if response[0] == 998:
                    forced_timeout = 1.5
                    data_list.append(response[3])
                if response[0] != 200:
                    print(response[0])
                    data_list.append(response[3])
                else:
                    results.append([response[1], response[3]])
            await asyncio.sleep(forced_timeout)

    return results


def run():
    """Update Summoners without IDs.

    Cycles through packages of data from the DB, pulling results
    and updating the package.
    """
    while True:  # Basic application loop
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)

            r = redis.StrictRedis(host='redis', port=6379, db=0, charset="utf-8", decode_responses=True)

            while True:  # Basic work loop after connection is established.
                messages = []
                passed = 0
                while len(messages) < 150:
                    message = channel.basic_get(
                            queue=f'LEAGUE_{server}')
                    if all(x is None for x in message):  # Empty Queue
                        break
                        
                    content = json.loads(message[2])
                    summonerId = content['summonerId']
                    latest = r.hgetall(f"user:{summonerId}")
                    print(latest.keys())
                        

                    if latest:
                        passed += 1
                        package = {**message[2], **latest}
                        channel.basic_publish(exchange='',
                                          routing_key=f'SUMMONER_{server}',
                                          body=json.dumps(package))
                        channel.basic_ack(
                            delivery_tag=message[0].delivery_tag)
                        continue

                    messages.append([message[0], message[1], content])

                if len(messages) == 0:
                    print("Empty")
                    time.sleep(1)
                    continue
                print(f"Passed a total of {passed} messages"
                      f" before reaching the limit of 250.")
                results = asyncio.run(main(messages))
                print(f"Adding {len(results)} player.")
                for result in results:
                    message = result[1]
                    data = result[0]
                    r.hset(f"user:{message[2]['summonerId']}",
                           mapping={'puuid': data['puuid'],
                                    'accountId': data['accountId']})
                    channel.basic_ack(
                        delivery_tag=message[0].delivery_tag)

                    package = {**message[2], **data}

                    channel.basic_publish(exchange='',
                                          routing_key=f'SUMMONER_{server}',
                                          body=json.dumps(package))

        except ClientConnectorError:
            # Raised when the proxy cant be reached
            print("Failed to reach Proxy")
            time.sleep(1)

        except RuntimeError:
            # Raised when rabbitmq cant connect
            print("Failed to reach rabbitmq")
            time.sleep(1)




if __name__ == "__main__":

    run()
