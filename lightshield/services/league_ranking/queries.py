preexisting = """SELECT summoner_id,
                        leaguepoints
                        FROM "ranking_{platform_lower:s}"
                        WHERE rank = $1
                        AND division = $2
                        """

update = """INSERT INTO "ranking_{platform_lower:s}" 
                                (summoner_id, platform, rank, division, leaguepoints)
                                VALUES ($1, '{platform:s}', $2, $3, $4)
                                ON CONFLICT (summoner_id, platform) DO 
                                UPDATE SET  rank = EXCLUDED.rank,
                                            division = EXCLUDED.division,
                                            leaguepoints = EXCLUDED.leaguepoints,
                                            last_updated = NOW()
                            """
