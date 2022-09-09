tasks = """
               UPDATE "match_{platform_lower:s}"
               SET update_reserved = NOW() + '30 minutes'::INTERVAL
               WHERE match_id  IN (
                   SELECT  match_id
                   FROM "match_{platform_lower:s}"
                       WHERE details IS NULL
                        AND find_fails < 10
                        AND (update_reserved IS NULL OR update_reserved <= NOW())
                    ORDER BY find_fails NULLS FIRST,
                            match_id DESC 
                   LIMIT $1
               )
               RETURNING match_id
               """

flush_found = """UPDATE "match_{platform_lower:s}"
                SET queue = $1,
                    timestamp = $2,
                    version = $3,
                    duration = $4,
                    win = $5,
                    details = TRUE,
                    update_reserved = NULL
                    WHERE match_id = $6
                """

flush_missing = """UPDATE "match_{platform_lower:s}"
                                   SET  find_fails = find_fails + 1,
                                        update_reserved = NULL
                                   WHERE match_id = ANY($1::BIGINT[])
                                """

summoners_update_only = """UPDATE summoner
                    SET last_activity = $1,
                        platform = $2,
                        name = $3,
                        last_updated = NOW()
                    WHERE puuid = $4 
                        AND last_activity < $1
                """

# TODO: Update queries to allow new users to be added
summoners_insert = """
                    UPDATE summoner
                    SET last_activity = $1,
                        platform = $2,
                        name = $3
                    WHERE puuid = $4 
                        AND last_activity < $1
                """
