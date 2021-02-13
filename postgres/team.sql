CREATE TABLE IF NOT EXISTS team
(
    match_id          BIGINT PRIMARY KEY,
    side              BOOLEAN DEFAULT NULL, -- False: Blue | True: Red
    tower_kills       SMALLINT,
    inhibitor_kills   SMALLINT,

    first_tower       BOOLEAN,
    first_rift_herald BOOLEAN,
    first_dragon      BOOLEAN,
    first_baron       BOOLEAN,

    rift_herald_kills SMALLINT,
    dragon_kills      SMALLINT,
    baron_kills       SMALLINT

)
