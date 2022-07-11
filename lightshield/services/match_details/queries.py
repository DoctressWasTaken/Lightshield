lock = {
    "postgres": """
               SELECT  match_id
               FROM "match_{platform_lower:s}"
                   WHERE details IS NULL
                    AND find_fails < 10
                ORDER BY find_fails 
               LIMIT $1
               FOR UPDATE 
               SKIP LOCKED
               """,
    "crate": """
                UPDATE "{schema}"."match"
                    SET "lock_details" = NOW() + INTERVAL '10 minutes'
                WHERE platform = '{platform:s}'
                    AND match_id IN (
                        SELECT match_id
                        FROM lightshield."match"
                        WHERE platform = '{platform:s}'
                            AND ("lock_details" IS NULL
                            OR "lock_details" < NOW())
                    LIMIT $1)
                RETURNING match_id
               """
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
    "crate": """UPDATE "{schema}"."match"
                SET queue = $1,
                    timestamp = $2,
                    version = $3,
                    duration = $4,
                    win = $5,
                    details = TRUE,
                    lock_details = NULL
                    WHERE match_id = $6
                """,
}

flush_missing = {
    "postgres": """UPDATE "match_{platform_lower:s}"
                                   SET find_fails = find_fails + 1
                                   WHERE match_id = $1
                                """,
    "crate": """UPDATE "{schema}"."match"
                                   SET find_fails = find_fails + 1,
                                       lock_details = NULL
                                   WHERE match_id = $1
                                """
}

flush_updates = {
    "postgres": """UPDATE summoner
                    SET last_activity = $1,
                        platform = $2,
                        name = $3
                    WHERE puuid = $4 
                        AND last_activity < $1
                """,
    "crate": """UPDATE "{schema}".summoner
                    SET last_activity = $1,
                        platform = $2,
                        name = $3
                    WHERE puuid = $4 
                        AND last_activity < $1
                """
}
