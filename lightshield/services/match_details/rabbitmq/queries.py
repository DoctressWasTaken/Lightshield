tasks = {
    "postgres": """
               SELECT  match_id
               FROM "match_{platform_lower:s}"
                   WHERE details IS NULL
                    AND find_fails < 10
                ORDER BY find_fails 
               LIMIT $1
               FOR SHARE 
               SKIP LOCKED
               """,
    "crate": """
                SELECT match_id
                FROM "{schema:s}"."match"
                WHERE platform = '{platform:s}'
                    AND details IS NULL
                    AND find_fails < 10
                ORDER BY find_fails
                LIMIT $1
                """,
}

flush_found = {
    "postgres": """UPDATE "match_{platform_lower:s}"
                SET queue = $1,
                    timestamp = $2,
                    version = $3,
                    duration = $4,
                    win = $5,
                    details = TRUE
                    WHERE match_id = $6
                """,
    "crate": """UPDATE "{schema:s}"."match"
                SET queue = $1,
                    timestamp = $2,
                    version = $3,
                    duration = $4,
                    win = $5,
                    details = TRUE
                    WHERE match_id = $6
                """,
}

flush_missing = {
    "postgres": """UPDATE "match_{platform_lower:s}"
                                   SET find_fails = find_fails + 1
                                   WHERE match_id = $1
                                """,
    "crate": """UPDATE "{schema:s}"."match"
                                   SET find_fails = find_fails + 1
                                   WHERE match_id = $1
                                    AND platform = '{platform:s}'
                                """,
}

summoners_update_only = {
    "postgres": """UPDATE summoner
                    SET last_activity = $1,
                        platform = $2,
                        name = $3
                    WHERE puuid = $4 
                        AND last_activity < $1
                """,
    "crate": """UPDATE "{schema:s}".summoner
                    SET last_activity = $1,
                        platform = $2,
                        name = $3
                    WHERE puuid = $4 
                        AND last_activity < $1
                """,
}
# TODO: Update queries to allow new users to be added
summoners_insert = {
    "postgres": """
                    UPDATE summoner
                    SET last_activity = $1,
                        platform = $2,
                        name = $3
                    WHERE puuid = $4 
                        AND last_activity < $1
                """,
    "crate": """INSERT INTO "{schema:s}".summoner (last_activity, platform, name, puuid)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (puuid, part) DO UPDATE
                SET name = EXCLUDED.name,
                    last_activity = EXCLUDED.last_activity,
                    platform = EXCLUDED.platform                    
                    WHERE last_activity < EXCLUDED.last_activity
                """,

}
