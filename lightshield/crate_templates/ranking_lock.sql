DROP TABLE IF EXISTS "{{schema}}"."rankin_lock";
CREATE TABLE "{{schema}}"."ranking_lock"
(
    summoner_id VARCHAR(63),
    platform    VARCHAR(4),
    created TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (summoner_id, platform)
) PARTITIONED BY (platform);
