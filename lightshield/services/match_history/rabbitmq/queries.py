reserve = {
    "postgres": """
                        SELECT  puuid, 
                                latest_match, 
                                last_history_update
                        FROM summoner
                        WHERE platform = $1
                            AND (
                                last_activity > last_history_update
                                OR last_history_update IS NULL)
                            AND (last_history_update < $2
                                OR last_history_update IS NULL)
                        ORDER BY last_history_update NULLS FIRST
                        LIMIT $3
                        FOR SHARE 
                        SKIP LOCKED
                    """,
    "crate": """
                SELECT  puuid,
                        latest_match,
                        last_history_update
                    FROM "{schema:s}".summoner
                    WHERE platform = $1
                        AND (
                            -- No update
                            last_history_update IS NULL
                            OR 
                            -- Update yes but nothing new
                            last_history_update < $2
                            OR 
                            -- Update yes and newer game found
                            (last_history_update < $3
                                AND last_activity > last_history_update
                            )
                        )
                    ORDER BY last_history_update NULLS FIRST
                    LIMIT $4
                    """,
}

insert_queue_known = {
    "postgres": """
                INSERT INTO "match_{platform_lower:s}" (platform, match_id, queue)
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
                INSERT INTO "match_{platform_lower:s}" (platform, match_id)
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
            """,
}
