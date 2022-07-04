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

    -- Ranked wins + losses
    games_sq       SMALLINT,
    games_fq       SMALLINT

);
-- summoner_id index included by default cause primary key
-- selector index for ranking - summoner link
CREATE INDEX ON PLATFORM.ranking (puuid);
-- selector index for puuid_collector service
CREATE INDEX ON PLATFORM.ranking ((puuid IS NULL));
