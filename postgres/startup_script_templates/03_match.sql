\connect lightshield;
CREATE TABLE IF NOT EXISTS REGION.match
(
    match_id  BIGINT PRIMARY KEY,
    platform  platform,
    queue     SMALLINT,
    timestamp TIMESTAMP,
    duration  SMALLINT DEFAULT NULL,
    win       BOOLEAN  DEFAULT NULL,
    details   BOOLEAN,
    timeline  BOOLEAN
);
-- General lookups
CREATE INDEX ON REGION.match ((timestamp::date), queue);
-- Lookup for undone tasks
-- This will only  be used if most data in the table is not null,
-- so to use it data older than your minimum match_details age should be removed
-- see https://stackoverflow.com/questions/5203755/why-does-postgresql-perform-sequential-scan-on-indexed-column
CREATE INDEX ON REGION.match ((details IS NULL));
CREATE INDEX ON REGION.match ((timeline IS NULL));

-- MATCH DETAILS
CREATE TABLE IF NOT EXISTS REGION.match_details
(
    match_id  BIGINT PRIMARY KEY,
    details   JSON
);

-- MATCH TIMELINE
CREATE TABLE IF NOT EXISTS REGION.match_timeline
(
    match_id  BIGINT PRIMARY KEY,
    timeline  JSON
);
