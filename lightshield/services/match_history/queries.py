get_tasks = """
                UPDATE summoner
                SET match_history_reserved = NOW() + '30 minutes'::INTERVAL
                WHERE platform = $1 
                    AND puuid IN ( 
                WITH base AS (
                    SELECT puuid,
                           latest_match,
                           last_history_update,
                           last_activity,
                           match_history_reserved,
                           platform,
                           last_history_update + '{min_activity_age:.0f} hour'::INTERVAL < last_activity AS found_activity,
                           DATE_PART('day', NOW() - last_history_update)                AS age
                    FROM summoner
                    WHERE platform = $1
                        AND (match_history_reserved IS NULL OR match_history_reserved < NOW())
                    ),
                    categories AS (
                    SELECT *,
                            CASE
                                WHEN found_activity AND age >= {found_newer_wait:.0f} THEN 2
                                WHEN age >= {no_activity_wait:.0f} THEN 3
                                WHEN age < {no_activity_wait:.0f} AND found_activity THEN 12
                                ELSE 13
                                END AS category
                    FROM base
                    )
                SELECT puuid
                FROM categories
                WHERE category < 10
                ORDER BY category, last_history_update
                LIMIT $2
                FOR UPDATE
                )
                RETURNING puuid, latest_match, last_history_update
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
                    last_history_update = $3,
                    last_activity = GREATEST(last_activity, $3),
                    last_updated = NOW(),
                    match_history_reserved = NULL
                WHERE puuid = $1
            """
