preexisting = {
    "postgres": """SELECT summoner_id,
                        rank,
                        division,
                        leaguepoints
                        FROM "ranking_{platform_lower:s}"
                        WHERE rank = $1
                        AND division = $2
                        """,
    "crate": """SELECT summoner_id,
                        rank,
                        division,
                        leaguepoints
                        FROM "{schema}"."ranking"
                        WHERE rank = $1
                        AND division = $2
                        """,
}

update = {
    "postgres": """INSERT INTO "ranking_{platform_lower:s}" 
                                (summoner_id, platform, rank, division, leaguepoints)
                                VALUES ($1, '{platform:s}', $2, $3, $4)
                                ON CONFLICT (summoner_id, platform) DO 
                                UPDATE SET  rank = EXCLUDED.rank,
                                            division = EXCLUDED.division,
                                            leaguepoints = EXCLUDED.leaguepoints,
                                            last_updated = NOW()
                            """,
    "crate": """INSERT INTO "{schema}"."ranking" 
                                (summoner_id, platform, rank, division, leaguepoints, last_updated)
                                VALUES ($1, '{platform:s}', $2, $3, $4, NOW())
                                ON CONFLICT (summoner_id, platform) DO 
                                UPDATE SET  rank = EXCLUDED.rank,
                                            division = EXCLUDED.division,
                                            leaguepoints = EXCLUDED.leaguepoints,
                                            last_updated = NOW()
                            """,
}
