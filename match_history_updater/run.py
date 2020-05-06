"""Match History updater. Pulls matchlists for all player."""

import psycopg2
import asyncio
import aiohttp
import json
import os
import time

if "SERVER" not in os.environ:
    print("No SERVER env variable provided. exiting")
    exit()

server = os.environ['SERVER']
config = json.loads(open('config.json').read())

# accountId, endIndex, beginIndex, queue
base_url = f"http://proxy:8000/match/v4/matchlists/by-account/" \
           f"%s?endIndex=%s&beginIndex=%s&queue=%s"


async def fetch(url, session, markers):
    """Call method."""
    async with session.get(url) as response:
        data = await response.json(content_type=None)
        return response.status, data, markers


async def call_data(data, session):
    """Generate and call match-history data.

    Generates the required urls and calls the api.
    Failed calls are repeated,
    the method returns once all calls are successfull.
    """
    games = max(data[1] + data[2] + 50,
                100)  # Pull an extra 50 matches to make sure none are lost
    id = data[0]
    marker = []
    pointer = 0
    while games > 0:
        if games > 100:
            marker.append(
                (pointer, pointer + 100))
            pointer += 100
            games -= 100
        else:
            marker.append(
                (pointer, pointer + games))
            games = 0

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
    """Async wrapper for api requests.

    Bundles requests for multiple player.
    Each player again requires multiple requests.
    """
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
    """Update user match lists.

    Wrapper function that starts the cycle.
    Pulls data from the DB in syncronous setup,
    calls requests in async method and uses the returned values to update.
    """
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
            WHERE diff_wins > 10
            OR diff_losses > 10
            LIMIT 100;
            """)
            data = cur.fetchall()
        if not data:
            time.sleep(5)
            continue
        matches, player, participations = asyncio.run(process_data(data))
        with psycopg2.connect(
                host=config['DB_HOST'],
                user=config['DB_USER'],
                dbname=config['DB_NAME']) as connection:
            cur = connection.cursor()
            cur.execute(f"""
                CREATE TEMP TABLE IF NOT EXISTS temp_{server}_matches (
                    match_id VARCHAR(30));
            """)
            connection.commit()
            lines = []

            for entry in matches:
                lines.append(
                    "('" + str(entry) + "')")
            print(f"Adding {len(lines)} matches.")
            query = f"""
                INSERT INTO temp_{server}_matches
                    (match_id)
                VALUES {",".join(lines)};
            """
            cur.execute(query)
            connection.commit()
            cur.execute(f"""
                INSERT INTO {server}_match
                    (match_id)
                SELECT * FROM temp_{server}_matches
                ON CONFLICT DO NOTHING;
                """)
            connection.commit()
            cur.execute(f"DROP TABLE temp_{server}_matches;")

            # ADD CONNECTIONS (Should not contain duplicates)
            lines = []
            for entry in participations:
                lines.append(
                    "('%s', %s, %s)" % (entry[0], entry[1], entry[2]))
            print(f"Adding {len(lines)} connections.")
            cur.execute(f"""
                INSERT INTO {server}_participation
                    (account_id, match_id, timestamp)
                VALUES {",".join(lines)}
                ON CONFLICT DO NOTHING;
                """)
            connection.commit()
            # UPDATE player
            lines = []
            for entry in player:
                lines.append(
                    "('%s', %s, %s)" % (entry[0], entry[1], entry[2]))
            print(f"Updating {len(lines)} player.")
            cur.execute(f"""
                UPDATE {server}_player
                SET
                    last_wins = v.last_wins,
                    last_losses = v.last_losses
                FROM ( VALUES
                    {",".join(lines)}
                    ) AS v(account_id, last_wins, last_losses)
                WHERE {server}_player.account_id = v.account_id;
                """)
            connection.commit()


if __name__ == "__main__":
    main()
