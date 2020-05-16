\connect data_na1;

CREATE TABLE IF NOT EXISTS player (
    summonerName VARCHAR(30),
    summonerId VARCHAR(50) PRIMARY KEY,
    accountId VARCHAR(56),
    puuid VARCHAR(78),
    ranking INTEGER,
    tier INTEGER,
    series VARCHAR(4),
    wins INTEGER,
    losses INTEGER
);
CREATE TABLE matchdto (

    matchId BIGINT PRIMARY KEY,
    participantIdentities json,
    queue INT,
    gameType INT, --Converting the text to number cause too long
    gameDuration INT,
    teams json,
    platformId VARCHAR(4),
    gameCreation BIGINT,
    seasonId INT,
    gameVersion VARCHAR(20),
    mapId INT,
    gameMode VARCHAR(15),
    participants json
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO db_worker;