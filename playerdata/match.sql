\connect playerdata;

CREATE TABLE IF NOT EXISTS EUW1_match (

    match_id VARCHAR(30) PRIMARY KEY

)

CREATE TABLE IF NOT EXISTS EUW1_participation (

    account_id VARCHAR(56),
    match_id VARCHAR(30),
    timestamp BIGINT

)