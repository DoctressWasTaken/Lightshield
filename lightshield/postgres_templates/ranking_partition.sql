CREATE TABLE "ranking_{platform:s}"
    PARTITION OF ranking
        FOR VALUES IN ('{platform_caps:s}');
CREATE INDEX ON "ranking_{platform:s}" (puuid);
CREATE INDEX ON "ranking_{platform:s}" ((puuid IS NULL));
