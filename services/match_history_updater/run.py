"""Match History updater. Pulls matchlists for all player."""

import asyncio
import json
import os
import time
import pika
import redis
import websockets

if "SERVER" not in os.environ:
    print("No SERVER env variable provided. exiting")
    exit()

server = os.environ['SERVER']

# accountId, beginIndex, endIndex, queue
base_url = "/match/v4/matchlists/by-account/" \
           "%s?beginIndex=%s&endIndex=%s&queue=%s"


async def process_data(tasked_user):
    """Async wrapper for api requests.

    Bundles requests for multiple player.
    Each player again requires multiple requests.
    """
    try:
        call_tasks = []
        user_responses = {}
        for user in tasked_user:
            data = user['content']
            calls_to_make = int(user['games_count']) // 100 + 1
            user_responses[data['accountId']] = {
                'content': data,
                'matches': [],
                'delivery_tag': user['delivery_tag']
            }
            for i in range(calls_to_make):
                params = [str(len(call_tasks)) + "_" + data['accountId'],
                          data['accountId'], i * 100,
                          (i + 1) * 100, 420]
                call_tasks.append(params)

        while len(call_tasks) > 0:
            message = {
                'endpoint': 'match',
                'url': base_url,
                'tasks': call_tasks
            }
            failed = []
            print("Making calls. Total:", len(call_tasks))
            async with websockets.connect("ws://proxy:6789") as websocket:
                remaining = len(call_tasks)
                print("Sending Name.")
                await websocket.send("Match_History_Updater")
                print("Sent name, sending data")
                await websocket.send(json.dumps(message))
                print("Sent data, awaiting.")
                while remaining > 0:
                    msg = await websocket.recv()
                    response_content = json.loads(msg)
                    if response_content['status'] == "rejected":
                        for task in response_content['tasks']:
                            remaining -= 1
                            failed.append(
                                call_tasks[
                                    task['id'].split("_", 1)[0]])

                    elif response_content['status'] == 'done':
                        for task in response_content['tasks']:
                            remaining -= 1
                            if task['status'] == 200:
                                for match in task['result']['matches']:
                                    if match['platformId'] == server \
                                            and match['queue'] == 420:
                                        user_responses[task['id']\
                                            .split("_", 1)[1]]['matches']\
                                            .append(
                                            match['gameId']
                                        )
                            else:
                                print(task['status'])
                                print(task['headers'])
                                print(task['result'])

            print(f"{len(failed)} tasks failed.")

            call_tasks = failed
        response = []
        for key in user_responses:
            response.append(user_responses[key])
        return response
    except Exception as err:
        print("Exception", err)


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
                queue=f'MATCH_HISTORY_IN_{server}',
                durable=True)

            # Outgoing
            channel.exchange_declare(
                exchange=f'MATCH_HISTORY_OUT_{server}',
                exchange_type='direct',
                durable=True
            )
            match_in = channel.queue_declare(
                queue=f'MATCH_IN_{server}',
                durable=True)
            channel.queue_bind(
                exchange=f'MATCH_HISTORY_OUT_{server}',
                queue=match_in.method.queue,
                routing_key='MATCH'
            )

            r = redis.StrictRedis(host='redis', port=6379, db=0,
                                  charset="utf-8", decode_responses=True)

            while True:
                total_calls = 0  # expected total calls to be made
                tasks = []
                skips = 0
                print("Starting cycle")
                while total_calls < 25:
                    message = channel.basic_get(
                        queue=f"MATCH_HISTORY_IN_{server}")

                    if all(x is None for x in message):
                        break

                    # Sort out too small
                    content = json.loads(message[2])
                    summonerId = content['summonerId']

                    prev = r.hgetall(f'user:{summonerId}')
                    games = content['wins'] + content['losses']
                    if prev:
                        games -= (int(prev['wins']) + int(prev['losses']))
                        if 0 < games < 10:
                            skips += 1

                    if games >= 10:
                        total_calls += games // 100 + 1
                        task = {
                            'delivery_tag': message[0].delivery_tag,
                            'content': content,
                            'games_count': games
                        }
                        tasks.append(task)

                if len(tasks) == 0:
                    print("Found no tasks, waiting.")
                    time.sleep(5)
                    continue

                print(f"Skipping {skips}.")
                print(f"Pulling data for {len(tasks)} user.")
                results = asyncio.run(
                    process_data(tasks))

                for result in results:
                    delivery_tag = result["delivery_tag"]
                    content = result["content"]
                    matches = result["matches"]
                    # Update/Create Redis entry

                    r.hset(f"user:{content['summonerId']}",
                           mapping={
                               "summonerName": content['summonerName'],
                               "wins": content['wins'],
                               "losses": content['losses'],
                               "tier": content['tier'],
                               "rank": content['rank'],
                               "leaguePoints": content['leaguePoints']
                           })
                    # Add Matches to outgoing Queue
                    for match_id in matches:
                        channel.basic_publish(
                            exchange=f'MATCH_HISTORY_OUT_{server}',
                            routing_key=f'MATCH',
                            body=str(match_id))
                    # Add User to outgoing Queue
                    # With all its changes
                    channel.basic_ack(
                        delivery_tag=delivery_tag)

        except RuntimeError:
            # Raised when rabbitmq cant connect
            print("Failed to reach rabbitmq")
            time.sleep(1)


if __name__ == "__main__":
    main()
