tasks = """SELECT summoner_id 
                   FROM "ranking_{platform_lower:s}"
                   WHERE puuid IS NULL
                   LIMIT $1   
    """

update_ranking = """UPDATE "ranking_{platform_lower:s}"
                    SET puuid = $2
                    WHERE summoner_id =  $1
                """

insert_summoner = """INSERT INTO summoner 
                    (puuid, name, last_activity, platform, last_updated)
                    VALUES($1, $2, $3, $4, NOW())
                    ON CONFLICT (puuid) 
                    DO NOTHING
                """

missing_summoner = """DELETE FROM "ranking_{platform_lower:s}"
                    WHERE summoner_id = $1
                """