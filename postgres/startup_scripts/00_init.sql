CREATE DATABASE lightshield;
GRANT ALL PRIVILEGES ON DATABASE lightshield TO postgres;
\connect lightshield;
CREATE TYPE platform AS ENUM ('EUW1', 'EUN1', 'TR1', 'RU', 'NA1', 'BR1', 'LA1', 'LA2', 'OC1', 'KR', 'JP1');
CREATE TYPE region AS ENUM ('europe', 'americas', 'asia');
CREATE TYPE rank AS ENUM ('IRON', 'BRONZE', 'SILVER', 'GOLD', 'PLATINUM', 'DIAMOND', 'MASTER', 'GRANDMASTER', 'CHALLENGER');
CREATE TYPE division AS ENUM ('I', 'II', 'III', 'IV');
