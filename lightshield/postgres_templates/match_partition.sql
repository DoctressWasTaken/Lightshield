CREATE TABLE "match_{platform:s}"
    PARTITION OF match
        FOR VALUES IN ('{platform_caps:s}');
-- General lookups
CREATE INDEX ON "match_{platform:s}" ((timestamp::date), queue);
-- Lookup for undone tasks
-- This will only  be used if most data in the table is not null,
-- so to use it data older than your minimum match_details age should be removed
-- see https://stackoverflow.com/questions/5203755/why-does-postgresql-perform-sequential-scan-on-indexed-column
CREATE INDEX ON "match_{platform:s}" ((details IS NULL), find_fails);
CREATE INDEX ON "match_{platform:s}" ((timeline IS NULL), find_fails);
