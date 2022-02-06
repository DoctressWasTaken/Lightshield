\connect lightshield;
CREATE TABLE IF NOT EXISTS REGION.match
(
    match_id          BIGINT,
    platform          platform,
    queue             SMALLINT,
    timestamp         TIMESTAMP,
    duration          SMALLINT DEFAULT NULL,
    win               BOOLEAN  DEFAULT NULL,
    -- Needed to avoid excluding correct match_ids as faulty/deleted becaues they aren't found first try
    -- Retries up to 10 times
    -- On a failed try the reserved counter is not reset to avoid immediate retries on a presumed faulty ID
    find_fails        SMALLINT DEFAULT 0,
    reserved_details  TIMESTAMP,
    reserved_timeline TIMESTAMP,
    details           BOOLEAN,
    timeline          BOOLEAN,
    PRIMARY KEY (platform, match_id)
);
-- General lookups
CREATE INDEX ON REGION.match ((timestamp::date), queue);
-- Lookup for undone tasks
-- This will only  be used if most data in the table is not null,
-- so to use it data older than your minimum match_details age should be removed
-- see https://stackoverflow.com/questions/5203755/why-does-postgresql-perform-sequential-scan-on-indexed-column
CREATE INDEX ON REGION.match ((details IS NULL), find_fails);
CREATE INDEX ON REGION.match ((timeline IS NULL), find_fails);

-- MATCH DETAILS
CREATE TABLE IF NOT EXISTS REGION.match_details
(
    platform platform,
    match_id BIGINT,
    details  JSON,
    PRIMARY KEY (platform, match_id)
);

-- MATCH TIMELINE
CREATE TABLE IF NOT EXISTS REGION.match_timeline
(
    platform platform,
    match_id BIGINT,
    timeline JSON,
    PRIMARY KEY (platform, match_id)
);
