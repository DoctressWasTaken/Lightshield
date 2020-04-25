import os
import psycopg2
from ratelimit import limits, RateLimitException
import aiohttp
import json
import asyncio
import time

config = json.loads(open('config.json').read())

headers = {'X-Riot-Token': config['API_KEY']}
if not "SERVER" in os.environ:
    print("No server provided, shutting down")
    exit()
server = os.environ['SERVER']

url_template = f"http://proxy:8000/summoner/v4/summoners/%s"

@limits(calls=250, period=10)
async def fetch(url, session):
    try:
        async with session.get(url, headers=headers) as response:
            resp = await response.json(content_type=None)
            if response.status == 429:
                print(response.headers)
            return response.status, resp, response.headers

    except Exception as err:
        print(err)
        return 999, [], {}

async def to_fetch(url, session, summoner_id):

    try:
        response = await fetch(url, session)
        return response + (summoner_id,)
    except RateLimitException:
        return 998, [], {}, summoner_id

async def main(data_list):
    results = []
    session = aiohttp.ClientSession()
    while data_list:
        forced_timeout = 0
        tasks = []
        tasks.append(asyncio.create_task(asyncio.sleep(10)))
        print("Starting call cycle")

        for i in range(min(250, len(data_list))):
            summoner_id = data_list.pop()
            tasks.append(
                asyncio.create_task(
                    to_fetch(
                        url_template % ( summoner_id),
                        session,
                        summoner_id
                        )
                    )
                )
            await asyncio.sleep(0.01)
        responses = await asyncio.gather(*tasks)
        responses.pop(0)
        for response in responses:
            if response[0] == 429:
                print("Got 429")
                if 'Retry-After' in response[2]:
                    forced_timeout = int(response[2]['Retry-After'])
                else:
                    forced_timeout = 1.5
                print(f"Waiting an additional {forced_timeout} seconds.")
            if response[0] == 998:
                forced_timeout = 1.5
            if response[0] != 200:
                print(response[0])
                data_list.append(response[3])
            else:
                results.append(response[1])
        await asyncio.sleep(forced_timeout)
    await session.close()
    return results


def run():
    
    while True:
        with psycopg2.connect(
                host=config['DB_HOST'],
                user=config['DB_USER'],
                dbname=config['DB_NAME']) as connection:
            connection.autocommit = True
            cur = connection.cursor()
            cur.execute(f"""
                SELECT summoner_id
                FROM {server}_player
                WHERE account_id IS NULL
                LIMIT 1200;
                """)
            data = cur.fetchall()
            if not data:
                print("No items to update found.")
                time.sleep(10)
                continue
            data_list = [entry[0] for entry in data]
            print(f"Found {len(data_list)} elements to upate.") 
            results = asyncio.run(main(data_list))
            print(f"Got {len(results)} elements.")
            cur.execute(f"""
            CREATE TEMP TABLE {server}_temp_ids
            AS
            SELECT summoner_id, account_id, puuid
            FROM {server}_player
            WHERE True=False;
            """)
            lines = []
            for entry in results:
                lines.append(
                        "('%s', '%s', '%s')" % (
                            entry['id'],
                            entry['accountId'],
                            entry['puuid']))
            query = f"""
                INSERT INTO {server}_temp_ids
                    (summoner_id, account_id, puuid)
                VALUES {",".join(lines)};
                """
            cur.execute(query)
            cur.execute(f"SELECT Count(*) FROM {server}_temp_ids;")
            print(cur.fetchall())

            cur.execute(f"""
                INSERT INTO {server}_player
                    (summoner_id, account_id, puuid)
                 SELECT * FROM {server}_temp_ids
                 ON CONFLICT (summoner_id) DO UPDATE
                 SET account_id=EXCLUDED.account_id,
                 puuid=EXCLUDED.puuid;
                 """)
            cur.execute(f"""DROP TABLE {server}_temp_ids;""")
        connection.close() 

if __name__ == "__main__":

    run()
