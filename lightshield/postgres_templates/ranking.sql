DROP TABLE IF EXISTS ranking CASCADE;
CREATE TABLE ranking
(
    summoner_id    VARCHAR(63),
    platform       platform,
    puuid          VARCHAR(78),

    rank           rank,
    division       division,
    leaguepoints   SMALLINT,

    -- Ranked wins + losses
    games_sq       SMALLINT,
    games_fq       SMALLINT,

    -- Last updated value
    last_updated   TIMESTAMP,
    update_reserved TIMESTAMP,
    PRIMARY KEY (summoner_id, platform)
)
    PARTITION BY LIST (platform)
;
