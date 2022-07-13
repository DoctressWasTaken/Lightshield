tasks = {
    "postgres": """SELECT summoner_id 
                   FROM "ranking_{platform:s}"
                   WHERE puuid IS NULL
                   LIMIT $1 
                   FOR SHARE 
                   SKIP LOCKED    
    """,
    "crate": """
        SELECT summoner_id
        FROM lightshield.ranking
        WHERE puuid IS NULL
          AND platform = 'EUW1'
        LIMIT 4000
    """
}

update_ranking = {
    "postgres": """UPDATE "ranking_{platform:s}"
                    SET puuid = $2
                    WHERE summoner_id =  $1
                """,
    "crate": """UPDATE "{schema:s}".ranking
                SET puuid = $2
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
                    ON CONFLICT (puuid, part) 
                    DO NOTHING
                """
}
missing_summoner = {
    "postgres": """DELETE FROM "ranking_{platform_lower:s}"
                    WHERE summoner_id = $1
                """,
    "crate": """DELETE FROM "{schema:s}".ranking
                    WHERE platform = '{platform:s}'
                    AND summoner_id = $1
    """
}
