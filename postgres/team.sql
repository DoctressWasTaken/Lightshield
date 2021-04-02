CREATE TABLE IF NOT EXISTS kr.team
(
    match_id          BIGINT,
    timestamp         TIMESTAMP,
    win               BOOLEAN,
    side              BOOLEAN, -- False: Blue | True: Red

    bans              SMALLINT[5],

    tower_kills       SMALLINT,
    inhibitor_kills   SMALLINT,

    first_tower       BOOLEAN,
    first_rift_herald BOOLEAN,
    first_dragon      BOOLEAN,
    first_baron       BOOLEAN,

    rift_herald_kills SMALLINT,
    dragon_kills      SMALLINT,
    baron_kills       SMALLINT,

    PRIMARY KEY (match_id, side)
);

CREATE INDEX ON kr.team (timestamp);
