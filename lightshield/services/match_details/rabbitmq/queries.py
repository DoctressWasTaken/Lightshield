tasks =  """
               SELECT  match_id
               FROM "match_{platform_lower:s}"
                   WHERE details IS NULL
                    AND find_fails < 10
                ORDER BY find_fails 
               LIMIT $1
               """

flush_found = """UPDATE "match_{platform_lower:s}"
                SET queue = $1,
                    timestamp = $2,
                    version = $3,
                    duration = $4,
                    win = $5,
                    details = TRUE
                    WHERE match_id = $6
                """

flush_missing = """UPDATE "match_{platform_lower:s}"
                                   SET find_fails = find_fails + 1
                                   WHERE match_id = ANY($1::INT[])
                                """

summoners_update_only = """UPDATE summoner
                    SET last_activity = $1,
                        platform = $2,
                        name = $3
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
