DROP TABLE IF EXISTS "{{schema}}"."ranking";
CREATE TABLE "{{schema}}"."ranking"
(
    summoner_id  VARCHAR(63),
    platform     VARCHAR(4),
    puuid        VARCHAR(78),

    rank         VARCHAR,
    division     VARCHAR,
    leaguepoints SMALLINT,

    -- Ranked wins + losses
    games_sq     SMALLINT,
    games_fq     SMALLINT,

    -- Last updated value
    last_updated TIMESTAMP,
    -- Updating key
    lock         TIMESTAMP,
    PRIMARY KEY (summoner_id, platform)
) PARTITIONED BY (platform);