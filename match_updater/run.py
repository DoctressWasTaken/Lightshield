"""Match History updater. Pulls matchlists for all player."""

import asyncio
import aiohttp
import json
import os
import time
import pika
import redis
from aiohttp.client_exceptions import ClientConnectorError

if "SERVER" not in os.environ:
    print("No SERVER env variable provided. exiting")
    exit()

server = os.environ['SERVER']

# matchId
base_url = f"http://proxy:8000/match/v4/matches/%s"


async def fetch(url, session, message):
    """Call method."""
    async with session.get(url) as response:
        try:
            data = await response.json(content_type=None)
        except:
            print(await response.text())
        return response.status, data, message

async def process_data(matches):
    """Async wrapper for api requests.

    Bundles requests for multiple player.
    Each player again requires multiple requests.
    """
    async with aiohttp.ClientSession() as session:
        for i in range(30):
            tasks = []
            results = []
            while matches:
                match = matches.pop()
                tasks.append(
                    asyncio.create_task(
                        fetch(
                            base_url % (match[2].decode('utf-8')),
                            session,
                            match
                        )
                    )
                )
                await asyncio.sleep(0.02)
            responses = await asyncio.gather(*tasks)
            forced_timeout = 0
            for response in responses:
                if response[0] == 404:
                    results.append({
                        "data": None,
                        "message": response[2]})
                elif response[0] == 428:
                    forced_timeout = 1.5
                elif response[0] != 200:
                    print(response[0])
                    matches.append(response[2])
                else:
                    results.append({
                        "data": response[1],
                        "message": response[2]})
            if len(matches) == 0:
                break
            print("Waiting for", forced_timeout + i)
            await asyncio.sleep(forced_timeout + i)

        return results


def main():
    """Update user match lists.

    Wrapper function that starts the cycle.
    Pulls data from the DB in syncronous setup,
    calls requests in async method and uses the returned values to update.
    """
    # Pull data package
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)
            # Incoming
            queue = channel.queue_declare(
                queue=f'MATCH_IN_{server}',
                durable=True)

            # Outgoing
            channel.exchange_declare(
                exchange=f'MATCH_OUT_{server}',
                exchange_type='direct',
                durable=True
            )
            # Queue for Matches
            db_in = channel.queue_declare(
                queue=f'DB_MATCH_IN_{server}',
                durable=True)
            channel.queue_bind(
                exchange=f'MATCH_OUT_{server}',
                queue=db_in.method.queue,
                routing_key='MATCH'
            )

            r = redis.StrictRedis(host='redis', port=6379, db=0,
                                  charset="utf-8", decode_responses=True)

            while True:
                tasks = []
                skips = 0
                print("Starting cycle")
                while len(tasks) < 400:
                    message = channel.basic_get(
                        queue=f"MATCH_IN_{server}")

                    if all(x is None for x in message):
                        break

                    match_id = str(message[2].decode('utf-8'))
                    if r.sismember('matches', str(match_id)):
                        skips += 1
                        continue

                    tasks.append(message)

                if len(tasks) == 0:
                    print("Found no tasks, waiting.")
                    time.sleep(5)
                    continue

                print(f"Skipping {skips}.")
                print(f"Pulling data for {len(tasks)} matches.")
                results = asyncio.run(
                    process_data(tasks))

                for entry in results:
                    message = entry["message"]
                    match = entry["data"]
                    # Update/Create Redis entry
                    match_id = str(message[2].decode('utf-8'))

                    r.sadd('matches', str(match_id))
                    if match:
                        # Add Match to outgoing Queue
                        channel.basic_publish(
                            exchange=f'MATCH_OUT_{server}',
                            routing_key=f'MATCH',
                            body=json.dumps(match))
                    # Add User to outgoing Queue
                    # With all its changes
                    channel.basic_ack(
                        delivery_tag=message[0].delivery_tag)


        except ClientConnectorError:
            # Raised when the proxy cant be reached
            print("Failed to reach Proxy")
            time.sleep(1)

        except RuntimeError:
            # Raised when rabbitmq cant connect
            print("Failed to reach rabbitmq")
            time.sleep(1)


if __name__ == "__main__":
    main()
