CREATE TABLE IF NOT EXISTS PLATFORM.participant
(
    match_id BIGINT,
    puuid    VARCHAR(78),
    champ    SMALLINT,
    team     BOOLEAN,
    PRIMARY KEY (match_id, puuid)
);
CREATE INDEX ON PLATFORM.participant(puuid);
