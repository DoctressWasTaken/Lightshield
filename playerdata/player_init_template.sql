CREATE DATABASE playerdata_direct WITH OWNER api;
GRANT ALL PRIVILEGES ON  DATABASE playerdata_direct TO api;

\connect playerdata_direct;
CREATE TABLE IF NOT EXISTS {SERVER}_player (
    summoner_name VARCHAR(30),
    summoner_id VARCHAR(50),
    account_id VARCHAR(56),
    puuid VARCHAR(78),
    ranking INTEGER,
    series VARCHAR(4),
    wins INTEGER,
    losses INTEGER,
    PRIMARY KEY(server, summoner_id)

    --Last updated values

    last_summoner_name VARCHAR(30),
    last_ranking INTEGER DEFAULT 0,
    last_wins INTEGER DEFAULT 0,
    last_losses INTEGER DEFAULT 0,

    -- Searchvalues

    diff_ranking INTEGER GENERATED ALWAYS AS (ranking - last_ranking),
    diff_wins INTEGER GENERATED ALWAYS AS (wins - last_wins),
    diff_losses INTEGER GENERATED ALWAYS AS (losses - last_losses)
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO api;
