reserve = {
    "postgres": """
                        SELECT  puuid, 
                                latest_match, 
                                last_history_update
                        FROM summoner
                        WHERE platform = '{schema:s}'
                            AND (
                                last_activity > last_history_update
                                OR last_history_update IS NULL)
                            AND (last_history_update + INTERVAL '1 days' * $1 < CURRENT_DATE
                            OR last_history_update IS NULL)
                        ORDER BY last_history_update NULLS FIRST
                        LIMIT 200
                        FOR SHARE 
                        SKIP LOCKED
                    """,
    "crate": """
                        UPDATE "{schema:s}".summoner
                        SET lock = NOW() + INTERVAL '10 minutes'
                        WHERE puuid IN (
                            SELECT puuid 
                            FROM "{schema:s}".summoner
                            WHERE platform = '{platform:s}'
                                AND ("lock" IS NULL OR "lock" < NOW())
                                AND (last_history_update + INTERVAL '{min_wait:n}' DAY < NOW()
                                    OR last_history_update IS NULL)
                                AND (last_activity > last_history_update
                                    OR last_history_update IS NULL)
                                AND $1::INT = $1
                            ORDER BY last_history_update NULLS FIRST
                            LIMIT 200
                            )
                        RETURNING puuid;    
                    """
}

insert_queue_known = {
    "postgres": """
                INSERT INTO "match_{platform:s}" (platform, match_id, queue)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
            """,
    "crate": """
                INSERT INTO "{schema:s}"."match" (platform, match_id, queue)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
            """,

}

insert_queue_unknown = {
    "postgres": """
                INSERT INTO "match_{platform:s}" (platform, match_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
            """,
    "crate": """
                INSERT INTO "{schema:s}"."match" (platform, match_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
            """,
}

update_players = {
    "postgres": """
                UPDATE summoner
                SET latest_match = $2,
                    last_history_update = $3
                WHERE puuid = $1
            """,
    "crate": """
                UPDATE "{schema:s}".summoner
                SET latest_match = $2,
                    last_history_update = $3
                WHERE puuid = $1
            """
}
