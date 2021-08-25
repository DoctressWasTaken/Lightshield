CREATE TABLE IF NOT EXISTS kr.summoner
(

    summoner_id           VARCHAR(63) PRIMARY KEY,
    account_id            VARCHAR(56),
    puuid                 VARCHAR(78),

    rank                  SMALLINT,

    -- Ranked wins + losses
    games_sq              SMALLINT,
    games_sq_last_updated SMALLINT,
    games_fq              SMALLINT,
    games_fq_last_updated SMALLINT,

    priority              VARCHAR(1),

    -- Update based on timestamp
    last_updated          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Latest match id to know if more requests are required for a full history
    last_matchid          BIGINT
);

-- Improved lookup speed for summoner_id service
-- and match_history service (full refresh tasks)
CREATE INDEX ON kr.summoner ((account_id IS NULL), (last_updated IS NULL));
