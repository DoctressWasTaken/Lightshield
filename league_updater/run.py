import psycopg2
import asyncio
import aiohttp
import json
import logging
import threading
import os

logging.basicConfig(
        format='%(asctime)s %(message)s', 
        datefmt='%m/%d/%Y %I:%M:%S %p',
        level=logging.DEBUG)
if not 'SERVER' in os.environ:
    print("No server provided, exiting.")
    exit()

server = os.environ['SERVER']
config = json.loads(open('../config.json').read())
headers = {'X-Riot-Token': config['API_KEY']}

url = f"https://{server}.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s"


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
    cur.execute(query);
    con.commit()

    cur.execute("""DROP TABLE temp_player;""")
    con.commit()
    logging.info("Done")


def dump_task(data, prev):

    if prev:
        prev.join()
    prev = threading.Thread(target=dump_data, args=(data,))
    prev.start()
    return prev


async def fetch(url, session, headers, page):
    try:
        print(f"Fetching {url}")
        async with session.get(url, headers=headers) as response:
            resp = await response.json()
            return response.status, resp, response.headers, page
    except Exception as err:
        print(err)
        return 999, [], {}, page


async def server_updater():
    data_sets = []
    thread = None
    loop = asyncio.get_event_loop()
    logging.info(f"Starting cycle")
    for tier in reversed(tiers):
        for division in divisions:
            if tier in ["MASTER", "GRANDMASTER", "CHALLENGER"]:
                if division != "I":
                    continue
            logging.info(f" Starting with {server} - {tier} - {division}")
            next_page = 1
            empty_count = 0
            page_batch = []
            while empty_count == 0 or page_batch:
                forced_timeout = 0
                while len(page_batch) < 5:
                    page_batch.append(next_page)
                    next_page += 1
                async with aiohttp.ClientSession() as session:
                    tasks = []
                    tasks.append(asyncio.create_task(asyncio.sleep(2)))
                    while page_batch:
                        page = page_batch.pop()
                        tasks.append(
                            asyncio.create_task(
                                fetch(
                                    url % (tier, division, page),
                                    session,
                                    headers,
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
            thread = await loop.run_in_executor(None, dump_task, data_sets, thread)
            data_sets = []

async def main():

    # Starting an additional task to not work faster than 1 cycle per hour
    while True:
        tasks = []
        tasks.append(
                asyncio.sleep(3600))
        tasks.append(
                asyncio.create_task(server_updater()))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
