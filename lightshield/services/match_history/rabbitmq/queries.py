reserve = """
                WITH base AS (
                    SELECT puuid,
                           latest_match,
                           last_history_update,
                           CASE
                               WHEN last_history_update IS NULL THEN 1
                               WHEN last_history_update < last_activity AND last_history_update < (NOW() - '1 day'::INTERVAL * $2)
                                   THEN 2
                               WHEN (last_history_update > last_activity OR last_activity IS NULL)
                                   AND last_history_update < (NOW() - '1 day'::INTERVAL * $3) THEN 3
                               END AS category
                    FROM summoner
                    WHERE platform = $1)
                SELECT puuid,
                       latest_match,
                       last_history_update
                FROM base
                WHERE category IS NOT NULL
                ORDER BY category, last_history_update
                LIMIT $4
                    """

insert_queue_known = """
                INSERT INTO "match_{platform_lower:s}" (platform, match_id, queue)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
            """

insert_queue_unknown = """
                INSERT INTO "match_{platform_lower:s}" (platform, match_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
            """

update_players = """
                UPDATE summoner
                SET latest_match = $2,
                    last_history_update = $3
                WHERE puuid = $1
            """
