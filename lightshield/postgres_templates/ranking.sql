DROP TABLE IF EXISTS ranking CASCADE;
CREATE TABLE ranking
(
    summoner_id  VARCHAR(63),
    platform     platform,
    puuid        VARCHAR(78),

    rank         rank,
    division     division,
    leaguepoints SMALLINT,

    -- Ranked wins + losses
    games_sq     SMALLINT,
    games_fq     SMALLINT

)
    PARTITION BY LIST (platform)
;
