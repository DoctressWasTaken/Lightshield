-- A platform specific table that pulls and updates data from the league endpoint.
-- A second central table will be used to connect rankings to actual players based on puuid
--  to accommodate the issue of rankings persisting in the API after a user swaps server
CREATE TABLE IF NOT EXISTS PLATFORM.ranking
(

    summoner_id    VARCHAR(63) PRIMARY KEY,
    puuid          VARCHAR(78),

    rank           rank,
    division       division,
    leaguepoints   SMALLINT,

    defunct        BOOLEAN    DEFAULT FALSE, -- summoner-v4 could not be found (swapped account)

    -- Ranked wins + losses
    games_sq       SMALLINT,
    games_fq       SMALLINT,

    -- Ranked wins + losses on last match_history pull
    last_games_sq  SMALLINT,
    last_games_fq  SMALLINT,

    -- Update based on timestamp
    last_updated   TIMESTAMP  DEFAULT CURRENT_TIMESTAMP

);
-- puuid_collector index included by default cause primary key
-- selector index for ranking - summoner link
CREATE INDEX ON PLATFORM.ranking (puuid);
-- selector index for puuid_collector service
CREATE INDEX ON PLATFORM.ranking ((puuid IS NULL));
