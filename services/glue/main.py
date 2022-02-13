import asyncio
import logging
import os

import asyncpg

if "DEBUG" in os.environ:
    logging.basicConfig(
        level=logging.DEBUG, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
else:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)8s %(asctime)s %(name)15s| %(message)s"
    )
logging.debug("Debug enabled.")

services = [
    "EUW1",
    "EUN1",
    "TR1",
    "RU",
    "NA1",
    "BR1",
    "LA1",
    "LA2",
    "OC1",
    "KR",
    "JP1",
]


async def sync_ranking():
    con = await asyncpg.connect(
        host="postgres", port=5432, user="postgres", database="lightshield"
    )
    ranking_query = []
    for platform in services:
        ranking_query.append("""
        SELECT DISTINCT puuid,
                last_updated,
                '%s'::varchar AS platform
                FROM %s.ranking
        """ % (platform, platform))

    await con.execute("""
        WITH preexisting AS (
                SELECT DISTINCT puuid
                FROM summoner
            ),
            rankings AS (
                %s
            ),
            unique_players AS (
                SELECT DISTINCT puuid,
                       FIRST_VALUE(platform) OVER (PARTITION BY puuid ORDER BY last_updated DESC) AS platform
                FROM rankings
                WHERE puuid IS NOT NULL
            )
        INSERT INTO summoner (puuid, last_platform)
        SELECT puuid, platform::platform FROM unique_players WHERE puuid NOT IN (SELECT puuid FROM preexisting)
    """ % 'UNION ALL'.join(ranking_query))
    logging.info("Synced ranking.")
    await con.close()


async def sync_participants():
    con = await asyncpg.connect(
        host="postgres", port=5432, user="postgres", database="lightshield"
    )
    ranking_query = []
    for platform in services:
        ranking_query.append("""
        SELECT puuid,
                MAX(match_id) AS last_match,
                '%s'::varchar AS platform
                FROM %s.participant
                GROUP BY puuid
        """ % (platform, platform))

    await con.execute("""
        WITH preexisting AS (
            SELECT DISTINCT puuid
            FROM summoner
            ),
            participants AS (
                %s
            ),
            last AS (
                SELECT DISTINCT puuid,
                        FIRST_VALUE(platform) OVER (PARTITION BY puuid ORDER BY last_match DESC) AS platform
                FROM participants
            )
        INSERT INTO summoner (puuid, last_platform)
                SELECT puuid, platform::platform FROM last WHERE puuid NOT IN (SELECT puuid FROM preexisting)             
    """ % 'UNION ALL'.join(ranking_query)
                      )
    await con.close()
    logging.info("Synced participants.")


async def main():
    while True:
        await asyncio.gather(
            asyncio.create_task(sync_participants()),
            asyncio.create_task(sync_ranking()))
        await asyncio.sleep(60 * 60 * 1)


if __name__ == '__main__':
    asyncio.run(main())
