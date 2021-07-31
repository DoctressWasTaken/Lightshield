CREATE TABLE IF NOT EXISTS kr.match
(
    match_id        BIGINT PRIMARY KEY,
    queue           SMALLINT,
    timestamp       TIMESTAMP,
    details_pulled  BOOLEAN
);
-- General lookups
CREATE INDEX ON kr.match ((timestamp::date), queue);
-- Lookup for undone tasks
-- This will only  be used if most data in the table is not null,
-- so to use it data older than your minimum match_details age should be removed
-- see https://stackoverflow.com/questions/5203755/why-does-postgresql-perform-sequential-scan-on-indexed-column
CREATE INDEX ON kr.match ((details_pulled IS NULL));

CREATE TABLE IF NOT EXISTS kr.match_data
(
    match_id  BIGINT PRIMARY KEY,
    queue     SMALLINT,
    timestamp TIMESTAMP,
    duration  SMALLINT DEFAULT NULL,
    win       BOOLEAN  DEFAULT NULL,
    details   JSON, -- If you want to reduce space required use JSON instead of JSONB
    timeline  JSON
);
-- General lookups
CREATE INDEX ON kr.match_data ((timestamp::date), queue, duration, win);
-- Ready for timeline index
CREATE  INDEX ON kr.match_data ((timeline IS NULL));
