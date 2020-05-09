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

# accountId, endIndex, beginIndex, queue
base_url = f"http://proxy:8000/match/v4/matchlists/by-account/" \
           f"%s?endIndex=%s&beginIndex=%s&queue=%s"


async def fetch(url, session, markers):
    """Call method."""
    async with session.get(url) as response:
        data = await response.json(content_type=None)
        return response.status, data, markers


async def call_data(el: dict, session):
    """Generate and call match-history data.

    Generates the required urls and calls the api.
    Failed calls are repeated,
    the method returns once all calls are successfull.
    """
    element = dict(el)
    games = element['games']
    games = max(games + 15,
                100)  # Pull an extra 15 matches to make sure none are lost
    games = (games // 100 + 1)  # Round up to the nearest 100
    content = element['content']

    id = content['accountId']
    marker = []
    for i in range(games):
        marker.append(
            (i * 100, (i + 1) * 100)
        )
    done = []
    while marker:
        tasks = []
        for entry in marker:
            url = base_url % (id, entry[1], entry[0], 420)
            tasks.append(
                asyncio.create_task(
                    fetch(url, session, entry)
                )
            )
            await asyncio.sleep(0.05)
        marker = []
        results = await asyncio.gather(*tasks)
        for entry in results:
            if entry[0] != 200:
                marker.append(entry[2])
            else:
                done.append(entry[1])
        if marker:
            await asyncio.sleep(2)

    matches = []
    for partial in done:
        matchlist = partial['matches']
        for match in matchlist:
            matches.append(match['gameId'])
    element['matches'] = matches
    return element


async def process_data(tasks):
    """Async wrapper for api requests.

    Bundles requests for multiple player.
    Each player again requires multiple requests.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for task in tasks:
            tasks.append(
                asyncio.create_task(
                    call_data(task, session)
                )
            )
        responses = await asyncio.gather(*tasks)

    return responses


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

            r = redis.Redis(host='redis', port=6379, db=0)

            while True:
                total_calls = 0  # expected total calls to be made
                tasks = []
                passed = 0
                while total_calls < 250:
                    message = channel.basic_get(
                        queue=f"SUMMONER_{server}")
                    if all(x is None for x in message):
                        break

                    content = json.loads(message[2])
                    summonerId = content['summonerId']

                    prev = r.hgetall(f'user:{summonerId}')
                    games = content['wins'] + content['losses']

                    changes = []

                    if prev:
                        games -= (prev['wins'] + prev['losses'])
                        if prev['summonerName'] != content['summonerName']:
                            changes.append("summonerName")
                        if prev['tier'] != content['tier'] or \
                            prev['rank'] != content['rank'] or \
                            prev['leaguePoints'] != content['leaguePoints']:
                            changes.append("ranking")
                        if 0 < games < 10:
                            changes.append("games_small")

                    if games >= 10:
                        changes.append("games")
                        total_calls += games // 100 + 1
                        task = {
                            'message': message,
                            'changes': changes,
                            'content': content,
                            'games': games
                        }
                        tasks.append(task)
                    elif changes:
                        pass
                        # Add User to outgoing User Queue to record changes

                results = asyncio.run(
                    process_data(tasks))

                for entry in results:
                    message = entry["message"]
                    content = entry["content"]
                    matches = entry["matches"]
                    changes = entry["changes"]
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
