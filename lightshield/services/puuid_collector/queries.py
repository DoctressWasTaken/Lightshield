tasks = {
    "postgres": """SELECT summoner_id 
                   FROM "ranking_{platform:s}"
                   WHERE puuid IS NULL
                   LIMIT $1 
                   FOR UPDATE 
                   SKIP LOCKED    
    """,
    "crate": """           
            INSERT INTO "{schema:s}".ranking_lock (summoner_id, platform)
            (
                SELECT summoner_id,
                       platform
                FROM "{schema:s}".ranking
                WHERE puuid IS NULL
                  AND platform = '{platform:s}'
                  AND summoner_id NOT IN (
                    SELECT summoner_id
                    FROM lightshield.ranking_lock
                    WHERE platform = '{platform:s}')
                LIMIT $1)
        ON CONFLICT DO NOTHING
        RETURNING summoner_id
    """
}

update_ranking = {
    "postgres": """UPDATE "ranking_{platform:s}"
                    SET puuid = $2
                    WHERE summoner_id =  $1
                """,
    "crate": """UPDATE "{schema:s}".ranking
                SET puuid = $2,
                    lock = NULL
                WHERE summoner_id = $1
    """
}

insert_summoner = {
    "postgres": """INSERT INTO summoner 
                    (puuid, name, last_activity, platform, last_updated)
                    VALUES($1, $2, $3, $4, NOW())
                    ON CONFLICT (puuid) 
                    DO NOTHING
                """,
    "crate": """INSERT INTO "{schema:s}".summoner 
                    (puuid, name, last_activity, platform, last_updated)
                    VALUES($1, $2, $3, $4, NOW())
                    ON CONFLICT (puuid) 
                    DO NOTHING
                """
}
missing_summoner = {
    "postgres": """DELETE FROM "ranking_{platform_lower:s}"
                    WHERE summoner_id = ANY($1::VARCHAR)
                """,
    "crate": """DELETE FROM "{schema:s}".ranking
                    WHERE platform = '{platform:s}'
                    AND summoner_id = ANY($1::VARCHAR)
    """
}
