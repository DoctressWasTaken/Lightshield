DROP TABLE IF EXISTS summoner;
CREATE TABLE summoner
(
    puuid                VARCHAR(78) PRIMARY KEY,
    name                 VARCHAR,

    -- Currently assumed platform
    platform             platform,

    -- Last time match-history was updated for the user
    last_history_update  TIMESTAMP DEFAULT NULL,
    latest_match         BIGINT DEFAULT NULL,
    -- Either through a match found or a summoner-v4 endpoint
    last_activity TIMESTAMP,
    -- summoner_tracker update timestamp. So it only updates every x days
    last_updated DATE
);

CREATE TABLE IF NOT EXISTS match_history_queue AS (
    SELECT puuid,
           latest_match,
           last_history_update,
           last_activity,
           platform,
           NULL::INT AS category,
           NOW() AS added
    FROM summoner
    LIMIT 0
);