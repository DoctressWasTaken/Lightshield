CREATE DATABASE lightshield;
GRANT ALL PRIVILEGES ON DATABASE lightshield TO postgres;
\connect lightshield;
CREATE TYPE platform AS ENUM ('EUW', 'EUNE', 'TR', 'RU', 'NA', 'BR', 'LAN', 'LAS', 'OCE', 'KR', 'JP');
CREATE TYPE region AS ENUM ('europe', 'americas', 'asia');
