DROP TABLE IF EXISTS "{{schema}}"."match";
CREATE TABLE "{{schema}}"."match"
(
    match_id    BIGINT,
    platform    VARCHAR,

    queue       SMALLINT,
    "timestamp" TIMESTAMP,
    version     SMALLINT,
    duration    SMALLINT DEFAULT NULL,
    win         BOOLEAN  DEFAULT NULL, -- Represents the winning team (0|1)

    -- Needed to avoid excluding correct match_ids as faulty/deleted becaues they aren't found first try
    -- Retries up to 10 times
    -- On a failed try the reserved counter is not reset to avoid immediate retries on a presumed faulty ID
    find_fails  SMALLINT DEFAULT 0,
    details     BOOLEAN,
    timeline    BOOLEAN,
    PRIMARY KEY (match_id, platform)
) PARTITIONED BY (platform);