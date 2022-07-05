CREATE TABLE "ranking_{platform:s}"
    PARTITION OF ranking
        FOR VALUES IN ('{platform_caps:s}');
ALTER TABLE "ranking_{platform:s}"
    ADD PRIMARY KEY (summoner_id);
CREATE INDEX ON "ranking_{platform:s}" (puuid);
CREATE INDEX ON "ranking_{platform:s}" ((puuid IS NULL));
