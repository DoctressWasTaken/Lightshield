DROP TABLE IF EXISTS match CASCADE;
CREATE TABLE IF NOT EXISTS match
(
    match_id   BIGINT,
    platform   platform,

    queue      SMALLINT,
    timestamp  TIMESTAMP,
    version    SMALLINT,
    duration   SMALLINT DEFAULT NULL,
    win        BOOLEAN  DEFAULT NULL, -- Represents the winning team (0|1)

    -- Needed to avoid excluding correct match_ids as faulty/deleted becaues they aren't found first try
    -- Retries up to 10 times
    -- On a failed try the reserved counter is not reset to avoid immediate retries on a presumed faulty ID
    find_fails SMALLINT DEFAULT 0,
    details    BOOLEAN,
    timeline   BOOLEAN,
    -- Shared reservation column for details and timeline
    update_reserved TIMESTAMP DEFAULT NULL,
    PRIMARY KEY (match_id, platform)
)
    PARTITION BY LIST (platform)
;