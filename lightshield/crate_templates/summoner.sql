DROP TABLE IF EXISTS "{{schema}}"."summoner";
CREATE TABLE "{{schema}}"."summoner"
(
    puuid               VARCHAR(78),
    name                VARCHAR,

    -- Currently assumed platform
    platform            VARCHAR,

    -- Last time match-history was updated for the user
    last_history_update TIMESTAMP DEFAULT NULL,
    latest_match        BIGINT    DEFAULT NULL,
    -- Either through a match found or a summoner-v4 endpoint
    last_activity       TIMESTAMP,
    -- summoner_tracker update timestamp. So it only updates every x days
    last_updated        TIMESTAMP,
    -- Updating key
    lock                TIMESTAMP,
    PRIMARY KEY (puuid)
);