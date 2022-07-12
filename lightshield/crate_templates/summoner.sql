DROP TABLE IF EXISTS "{{schema}}"."summoner";
CREATE TABLE "{{schema}}"."summoner"
(
    puuid               VARCHAR(78),
    part                GENERATED ALWAYS AS SUBSTR(puuid, 1, 1),
    name                VARCHAR,

    -- Currently assumed platform
    platform            VARCHAR,

    -- Last time match-history was updated for the user
    last_history_update TIMESTAMP DEFAULT NULL,
    latest_match        BIGINT    DEFAULT NULL,
    -- Either through a match found or a summoner-v4 endpoint
    last_activity       TIMESTAMP WITHOUT TIME ZONE,
    -- summoner_tracker update timestamp. So it only updates every x days
    last_updated        TIMESTAMP WITHOUT TIME ZONE,
    -- Updating key
    lock                TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (puuid, part)
) PARTITIONED BY (part);