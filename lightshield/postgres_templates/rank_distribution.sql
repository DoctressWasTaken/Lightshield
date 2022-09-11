DROP TABLE IF EXISTS rank_distribution;
CREATE TABLE rank_distribution
(
    platform     platform,
    rank         rank,
    division     division,
    last_updated TIMESTAMP,

    PRIMARY KEY (platform, rank, division)
);
