"""League Updater Module."""
import psycopg2
import asyncio
from tornado.httpclient import AsyncHTTPClient
import json
import logging
import threading
import os

logging.basicConfig(
        format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.DEBUG)
if 'SERVER' not in os.environ:
    print("No server provided, exiting.")
    exit()

server = os.environ['SERVER']
config = json.loads(open('config.json').read())

url = f"http://proxy:8000/league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"


tiers = [
        "IRON",
        "BRONZE",
        "SILVER",
        "GOLD",
        "PLATINUM",
        "DIAMOND",
        "MASTER",
        "GRANDMASTER",
        "CHALLENGER"]

divisions = [
        "IV",
        "III",
        "II",
        "I"]

map_tiers = {
        'IRON': 0,
        'BRONZE': 4,
        'SILVER': 8,
        'GOLD': 12,
        'PLATINUM': 16,
        'DIAMOND': 20,
        'MASTER': 24,
        'GRANDMASTER': 24,
        'CHALLENGER': 24}
map_divisions = {
        'IV': 0,
        'III': 1,
        'II': 2,
        'I': 3}


def dump_data(data):
    """Connect and insert data into the connected DB."""
    logging.info("Starting data dump")
    logging.info("Transforming data")
    sets = []
    for player_set in data:
        for player in player_set:
            ranking = map_tiers[player['tier']] * 100
            ranking += map_divisions[player['rank']] * 100
            ranking += player['leaguePoints']
            series = ""
            if 'miniSeries' in data:
                series = player['miniSeries']['progress'][:4]
            sets.append([
                player['summonerName'],
                player['summonerId'],
                str(ranking),
                series,
                str(player['wins']),
                str(player['losses'])])
    lines = []
    for s in sets:
        lines.append("'" + "','".join(s) + "'")
    lines = list(set(lines))
    logging.info("Adding to db")
    con = psycopg2.connect(
            host=config['DB_HOST'],
            user=config['DB_USER'],
            dbname=config['DB_NAME'])
    cur = con.cursor()
    cur.execute(f"""
        CREATE TEMP TABLE temp_player
        AS
        SELECT summoner_name, summoner_id, ranking, series, wins, losses
        FROM {server}_player
        WHERE True=False;
        """)
    con.commit()
    values = ""
    for entry in lines:
        values += "(" + entry + "),"
    values = values[:-1] + ";"
    cur.execute("""
        INSERT INTO temp_player
                (summoner_name, summoner_id, ranking, series, wins, losses)
        VALUES %s ;
        """ % (values))
    con.commit()

    query = f"""
            INSERT INTO {server}_player
                (summoner_name, summoner_id, ranking, series, wins, losses)
            SELECT * FROM temp_player
            ON CONFLICT (summoner_id) DO UPDATE SET
            summoner_name=EXCLUDED.summoner_name,
            ranking=EXCLUDED.ranking,
            wins=EXCLUDED.wins,
            losses=EXCLUDED.losses;
            """
    cur.execute(query)
    con.commit()

    cur.execute("""DROP TABLE temp_player;""")
    con.commit()
    logging.info("Done")
    con.close()


def dump_task(data):
    """Sync wrapper for the syncronized DB connector."""
    dump_data(data)


async def fetch(url, http_client, page):
    """Fetch data from the api.

    Returns Non-200 HTTP Code on error.
    """
    try:
        response = await http_client.fetch(url)
        data = json.loads(response.body)
        return response.code, data, response.headers.get_all(), page
    except Exception as err:
        print(err)
        return 999, [], {}, page


async def server_updater():
    """Get data.

    Core function that sets up call batches by rank category.
    """
    data_sets = []
    loop = asyncio.get_event_loop()
    logging.info(f"Starting cycle")
    http_client = AsyncHTTPClient()
    for tier in reversed(tiers):
        for division in divisions:
            if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                if division != "I":
                    continue
            logging.info(f" Starting with {server} - {tier} - {division}")
            logging.info(f"Active Threads: {threading.active_count()}")
            for thread in threading.enumerate():
                print(thread)
            next_page = 1
            empty_count = 0
            page_batch = []
            while empty_count == 0 or page_batch:
                forced_timeout = 0
                while len(page_batch) < 5:
                    page_batch.append(next_page)
                    next_page += 1
                tasks = []
                tasks.append(asyncio.create_task(asyncio.sleep(2)))
                while page_batch:
                    page = page_batch.pop()
                    tasks.append(
                        asyncio.create_task(
                            fetch(
                                url % (tier, division, page),
                                http_client,
                                page
                            )
                        )
                    )
                responses = await asyncio.gather(*tasks)
                responses.pop(0)
                for response in responses:
                    if response[0] == 429:
                        print("Got 429")
                        if 'Retry-After' in response[2]:
                            forced_timeout = int(response[2]['Retry-After'])
                        else:
                            forced_timeout = 2
                        print(f"Waiting n additional {forced_timeout}.")
                    if response[0] != 200:
                        print(response)
                        page_batch.append(response[3])
                    else:
                        if len(response[1]) == 0:
                            empty_count += 1
                        else:
                            data_sets.append(response[1])

                await asyncio.sleep(forced_timeout)
            print(f"Done with {server} - {tier} - {division};"
                  f" Pages: {next_page - 1}.")
            await loop.run_in_executor(None, dump_task, data_sets)
            data_sets = []


async def main():
    """Start loop to request data from the api and update the DB.

    The loop is limited to run once every 6 hours max.
    """
    while True:
        tasks = [asyncio.sleep(6 * 3600),
                 asyncio.create_task(server_updater())]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
