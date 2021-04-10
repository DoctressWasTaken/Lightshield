CREATE TABLE IF NOT EXISTS euw1.match
(
    match_id        BIGINT PRIMARY KEY,
    queue           SMALLINT,
    timestamp       TIMESTAMP,
    details_pulled  BOOLEAN,
    timeline_pulled BOOLEAN
);
-- General lookups
CREATE INDEX ON euw1.match ((timestamp::date), queue);
-- Lookup for undone tasks
-- This will only  be used if most data in the table is not null,
-- so to use it data older than your minimum match_details age should be removed
-- see https://stackoverflow.com/questions/5203755/why-does-postgresql-perform-sequential-scan-on-indexed-column
CREATE INDEX ON euw1.match ((details_pulled IS NULL));
CREATE INDEX ON euw1.match ((timeline_pulled IS NULL));

CREATE TABLE IF NOT EXISTS euw1.match_data
(
    match_id  BIGINT PRIMARY KEY,
    queue     SMALLINT,
    timestamp TIMESTAMP,
    duration  SMALLINT DEFAULT NULL,
    win       BOOLEAN  DEFAULT NULL,
    details   JSON, -- If you want to reduce space required use JSON instead of JSONB
    timeline  JSON,
    roleml    JSON
);
-- General lookups
CREATE INDEX ON euw1.match_data ((timestamp::date), queue, duration, win);
-- Ready for roleml index
CREATE INDEX ON euw1.match_data ((details IS NOT NULL AND timeline IS NOT NULL AND roleml IS NULL));
-- Find finished roleml
CREATE INDEX ON euw1.match_data ((roleml IS NOT NULL));
