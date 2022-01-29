\connect lightshield;
CREATE TABLE IF NOT EXISTS summoner(
    puuid VARCHAR(78) PRIMARY KEY,
    name VARCHAR(18),

    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_platform platform,
    last_match BIGINT
);
