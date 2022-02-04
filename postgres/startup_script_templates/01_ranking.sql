-- A platform specific table that pulls and updates data from the league endpoint.
-- A second central table will be used to connect rankings to actual players based on puuid
--  to accommodate the issue of rankings persisting in the API after a user swaps server
\connect lightshield;
CREATE TABLE IF NOT EXISTS PLATFORM.ranking
(

    summoner_id  VARCHAR(63) PRIMARY KEY,
    account_id   VARCHAR(56),
    puuid        VARCHAR(78),

    rank         rank,
    division     division,
    leaguepoints SMALLINT,

    defunct      BOOLEAN   DEFAULT FALSE, -- summoner-v4 could not be found (swapped account)

    -- Ranked wins + losses
    games_sq     SMALLINT,
    games_fq     SMALLINT,

    priority     VARCHAR(1),

    -- Update based on timestamp
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Improved lookup speed for summoner_id service
-- and match_history service (full refresh tasks)
CREATE INDEX ON PLATFORM.ranking ((account_id IS NULL), (last_updated IS NULL));
