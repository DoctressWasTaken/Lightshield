CREATE SCHEMA IF NOT EXISTS tracking;
DROP TABLE IF EXISTS tracking.requests;
CREATE TABLE tracking.requests
(
    response_type SMALLINT,
    request_count INT,
    interval_time TIMESTAMP,
    platform      VARCHAR,
    endpoint      VARCHAR,
    api_key       VARCHAR,
    PRIMARY KEY (response_type, interval_time, platform, endpoint, api_key)
);