\connect playerdata;
CREATE TABLE IF NOT EXISTS EUW1_player (
    summonerName VARCHAR(30),
    summonerId VARCHAR(50) PRIMARY KEY,
    accountId VARCHAR(56),
    puuId VARCHAR(78),
    ranking INTEGER,
    tier INTEGER,
    series VARCHAR(4),
    wins INTEGER,
    losses INTEGER
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO db_worker;
