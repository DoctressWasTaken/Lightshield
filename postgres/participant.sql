CREATE TABLE IF NOT EXISTS participant
(
    match_id                     BIGINT,
    timestamp                    TIMESTAMP,
    win                          BOOLEAN,

    participant_id               SMALLINT,
    summoner_id                  VARCHAR(63), -- current summoner id

    summoner_spell               SMALLINT[2],

    -- Runes
    rune_main_tree               VARCHAR(1),  -- First letter of main tree
    rune_sec_tree                VARCHAR(1),  -- First letter of secondary tree
    rune_main_select             SMALLINT,    -- 1-4 positions per rune
    rune_sec_select              SMALLINT,    -- 0-4 positions per rune
    rune_shards                  SMALLINT,    -- 1-3 positions per rune,

    -- Items
    item                         SMALLINT[6],
    trinket                      SMALLINT,

    -- Champ
    champ_level                  SMALLINT,
    champ_id                     SMALLINT,

    -- KDA
    kills                        SMALLINT,
    deaths                       SMALLINT,
    assists                      SMALLINT,

    -- Gold income
    gold_earned                  INT,
    neutral_minions_killed       SMALLINT,
    neutral_minions_killed_enemy SMALLINT,
    neutral_minions_killed_team  SMALLINT,
    total_minions_killed         SMALLINT,

    -- Vision
    vision_score                 SMALLINT,
    vision_wards_bought          SMALLINT,
    wards_placed                 SMALLINT,
    wards_killed                 SMALLINT,

    -- Damage taken
    physical_taken               INT,
    magical_taken                INT,
    true_taken                   INT,
    damage_mitigated             INT,

    -- Damage dealt (to champions)
    physical_dealt               INT,
    magical_dealt                INT,
    true_dealt                   INT,

    -- Other damage
    turret_dealt                 INT,
    objective_dealt              INT,

    -- Utility: Healing & CC
    total_heal                   INT,
    total_units_healed           SMALLINT,
    time_cc_others               SMALLINT,
    total_cc_dealt               SMALLINT,

    PRIMARY KEY (match_id, participant_id)
);

CREATE INDEX ON participant (summoner_id);
CREATE INDEX ON participant (rune_main_tree, rune_sec_tree);
CREATE INDEX ON participant (champ_id);
CREATE INDEX ON participant (timestamp);
