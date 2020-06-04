"""Summoner ID Updater Module."""

import os
import json
import asyncio
import time
import redis
import pika
import websockets

if "SERVER" not in os.environ:
    print("No server provided, shutting down")
    exit()
server = os.environ['SERVER']

url_template = "/summoner/v4/summoners/%s"


async def get_returns(remaining, websocket, data_list):
    failed = []
    data_sets = []
    not_found = []  # Should not be retried

    while remaining > 0:
        msg = await websocket.recv()
        data = json.loads(msg)
        if data['status'] == "rejected":
            for task in data['tasks']:
                remaining -= 1
                failed.append(data_list[task])
        elif data['status'] == 'done':
            for entry in data['tasks']:
                remaining -= 1
                if entry['status'] == 200:
                    if len(entry['result']) == 0:
                        empty = True
                    else:
                        data_sets.append([
                            data_list[entry['id']],
                            entry['result']])
                elif entry['status'] == 404:
                    not_found.append(data_list[entry['id']])
                else:
                    failed.append(data_list[entry['id']])
                    print(entry['status'])
                    print(entry['headers'])
        print(f"{remaining} tasks remaining.")

    return data_sets, failed, not_found


async def main(data_list):
    """Call data collectively.

    Function that bundles all API calls.
    Returns once all calls are finished.
    """
    print(f"Starting with {len(data_list)} tasks.")
    call_tasks = []
    for id, task in enumerate(data_list):
        call_tasks.append([id, task[2]['summonerId']])
    message = {
        "endpoint": "summoner",
        "url": url_template,
        "tasks": call_tasks
    }
    failed = []
    data_sets = []
    not_found = []  # Should not be retried
    async with websockets.connect("ws://proxy:6789") as websocket:
        remaining = len(call_tasks)
        await websocket.send("Summoner_Id_Updater")
        await websocket.send(json.dumps(message))
        try:
            data_sets, failed, not_found = await asyncio.wait_for(
                get_returns(remaining, websocket, data_list), 60)
        except asyncio.TimeoutError:
            print("Failed to retrieve all tasks in "
                  "the selected timeout period."
                  "Canceling all tasks.")

            return [], [element for element in data_list] ,[]

    # data_sets: List of lists each containing [original element, content]
    print(f"Finished with {len(data_list)} tasks.")
    return data_sets, failed, not_found


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

            # Incoming
            incoming = channel.queue_declare(
                queue=f'SUMMONER_ID_IN_{server}',
                durable=True)

            # Outgoing
            channel.exchange_declare(
                exchange=f'SUMMONER_ID_OUT_{server}',
                exchange_type='direct',
                durable=True
            )
            # Output to the Match_History_Updater
            match_history_in = channel.queue_declare(
                queue=f'MATCH_HISTORY_IN_{server}',
                durable=True
            )
            channel.queue_bind(
                exchange=f'SUMMONER_ID_OUT_{server}',
                queue=match_history_in.method.queue,
                routing_key='SUMMONER_V2'
            )
            # Output to the DB
            db_in = channel.queue_declare(
                queue=f'DB_SUMMONER_IN_{server}',
                durable=True)
            channel.queue_bind(
                exchange=f'SUMMONER_ID_OUT_{server}',
                queue=db_in.method.queue,
                routing_key='SUMMONER_V2'

            )
            # TODO: Adding logging output will happen here
            #  by binding a second queue that outputs only the change events

            r = redis.StrictRedis(host='redis', port=6379, db=0,
                                  charset="utf-8", decode_responses=True)
            messages = []
            while True:  # Basic work loop after connection is established.
                print("Starting basic loop cycle.")
                passed = 0
                while len(messages) < 1000:
                    message = channel.basic_get(
                        queue=f'SUMMONER_ID_IN_{server}')
                    if all(x is None for x in message):  # Empty Queue
                        break
                    content = json.loads(message[2])
                    if type(content) != dict or "summonerId" not in content:
                        channel.basic_ack(
                            delivery_tag=message[0].delivery_tag)
                        continue
                    summonerId = content['summonerId']
                    latest = r.hgetall(f"user:{summonerId}")

                    if latest:
                        passed += 1
                        package = {**content, **latest}

                        channel.basic_publish(
                            exchange=f'SUMMONER_ID_OUT_{server}',
                            routing_key=f'SUMMONER_V2',
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
                      f" before reaching the limit.")
                results, fails, not_found = asyncio.run(main(messages))
                print("Got responses")
                messages = []
                for result in not_found:
                    channel.basic_ack(
                        delivery_tag=result[0].delivery_tag)
                for result in fails:
                    messages.append(result)
                print(f"Adding {len(results)} player.")
                print("!")
                for result in results:
                    message = result[0]
                    data = result[1]
                    r.hset(f"user:{message[2]['summonerId']}",
                           mapping={'puuid': data['puuid'],
                                    'accountId': data['accountId']})
                    channel.basic_ack(
                        delivery_tag=message[0].delivery_tag)

                    package = {**message[2], **data}

                    channel.basic_publish(
                        exchange=f'SUMMONER_ID_OUT_{server}',
                        routing_key=f'SUMMONER_V2',
                        body=json.dumps(package))
                print("!")

        except RuntimeError:
            # Raised when rabbitmq cant connect
            print("Failed to reach rabbitmq")
            time.sleep(1)


if __name__ == "__main__":
    time.sleep(5)
    run()
