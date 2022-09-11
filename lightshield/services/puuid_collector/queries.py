tasks = """UPDATE "ranking_{platform_lower:s}"
            SET update_reserved = NOW() + '30 minutes'::INTERVAL
            WHERE summoner_id IN (
                    SELECT summoner_id 
                   FROM "ranking_{platform_lower:s}"
                   WHERE puuid IS NULL
                   AND (update_reserved IS NULL OR update_reserved < NOW())
                   LIMIT $1 
                   )
            RETURNING summoner_id  
    """

update_ranking = """UPDATE "ranking_{platform_lower:s}"
                    SET puuid = $2,
                    update_reserved = NULL
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
