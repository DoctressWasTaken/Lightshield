"""Match History updater. Pulls matchlists for all player."""

import psycopg2

import asyncio
import aiohttp
import threading
import queue
import json
import os
import time

if not "SERVER" in os.environ:
    print("No SERVER env variable provided. exiting")
    exit()

server = os.environ['SERVER']
config = json.loads(open('config.json').read())


# accountId, endIndex, beginIndex, queue
base_url = f"https://proxy:8000/match/v4/matchlists/by-account/%s?endIndex=%s&beginIndex=%s&queue=%s"


async def fetch(url, session):
    async with session.get(url) as response:
        data = await response.json(content_type=None)
        return response.status, data


async def call_data(data, session):
    games = max(data[1] + data[2] + 50, 100)  # Pull an extra 50 matches to make sure none are lost
    id = data[0]
    marker = []
    pointer = 0
    while games > 0:
        marker.append(pointer)
        if games > 100:
            pointer += 100
            games -= 100
        else:
            marker.append(pointer + games)
            games = 0

    done = []
    while marker:
        tasks = []
        for index, entry in enumerate(marker[:-1]):
            url = base_url % (id, marker[index+1], entry, 420)
            tasks.append(
                asyncio.create_task(
                    fetch(url, session)
                )
            )
        marker = []
        results = await asyncio.gather(*tasks)
        for entry in results:
            if entry[0] != 200:
                marker += entry[2]
            else:
                done.append(entry[1])
        if marker:
            marker = list(set(marker))
            marker.sort()
            await asyncio.sleep(2)
            continue

    matches = []
    participations = []
    for partial in done:
        matchlist = partial['matches']
        for match in matchlist:
            matches.append(match['gameId'])
            participations.append([
                id,
                match['gameId'],
                match['timestamp'] // 1000
            ])
    return matches, data, participations


async def process_data(data):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for entry in data:
            tasks.append(
                asyncio.create_task(
                    call_data(entry, session)
                )
            )
        responses = await asyncio.gather(*tasks)

    matches = []
    player = []
    participations = []
    for response in responses:
        matches += response[0]
        player.append(response[1])
        participations += response[2]

    matches = list(set(matches))

    return matches, player, participations




def main():
    # Pull data package
    while True:
        with psycopg2.connect(
                host=config['DB_HOST'],
                user=config['DB_USER'],
                dbname=config['DB_NAME']) as connection:
            cur = connection.cursor()
            cur.execute(f"""
            SELECT account_id, diff_wins, diff_losses
            FROM {server}_player
            WHERE diff_wins != 0
            OR diff_losses != 0
            LIMIT 100;
            """)
            data = cur.fetchall()
        if not data:
            time.sleep(5)
            continue
        matches, player, participations =  asyncio.run(data)
        with psycopg2.connect(
                host=config['DB_HOST'],
                user=config['DB_USER'],
                dbname=config['DB_NAME']) as connection:
            cur = connection.cursor()
            cur.execute(f"""
                CREATE TEMP TABLE temp_{server}_matches
                AS SELECT account_id
                FROM {server}_match
                WHERE True=False;
            """)
            connection.commit()
            lines = []

            for entry in matches:
                lines.append(
                    "('" + "', '".join(entry) + "')")

            query = f"""
                INSERT INTO temp_{server}_matches
                    (account_id)
                VALUES {",".join(lines)};
            """
            cur.execute(query)
            connection.commit()
            cur.execute(f"""
                INSERT INTO {server}_match
                    (account_id)
                SELECT * FROM temp_{server}_player
                ON CONFLICT DO NOTHING;
                """)
            connection.commit()
            cur.execute(f"DROP TABLE temp_{server}_player;")

            # UPDATE player
            lines = []
            for entry in player:
                lines.append(
                    "('" + "', '".join(entry) + "')")
            cur.execute(f"""
                INSERT INTO {server}_player
                    (account_id, last_wins, last_losses)
                VALUES {",".join(lines)}
                ON CONFLICT (account_id) DO UPDATE SET 
                last_wins=EXCLUDED.last_wins,
                last_losses=EXCLUDED.last_losses;
                """)
            connection.commit()
            # ADD CONNECTIONS (Should not contain duplicates)
            lines = []
            for entry in participations:
                lines.append(
                    "('" + "', '".join(entry) + "')")
            cur.execute(f"""
                INSERT INTO {server}_participation
                    (account_id, match_id, timestamp)
                VALUES {",".join(lines)}
                ON CONFLICT DO NOTHING;
                """)
            connection.commit()


if __name__ == "__main__":

    main()









