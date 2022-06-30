DROP TABLE IF EXISTS summoner;
CREATE TABLE summoner
(
    puuid                  VARCHAR(78) PRIMARY KEY,
    name                   VARCHAR(18),

    reserved_match_history TIMESTAMP DEFAULT NULL,
    last_updated           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_platform          platform,
    last_match             BIGINT
);

