DROP TABLE IF EXISTS "tracking"."requests";
CREATE TABLE "tracking"."requests"
(
    response_type VARCHAR,
    request_count SMALLINT,
    interval_time TIMESTAMP WITHOUT TIME ZONE,
    platform      VARCHAR,
    endpoint      VARCHAR,
    api_key       VARCHAR,
    PRIMARY KEY (response_type, interval_time, platform, endpoint, api_key)
) PARTITIONED BY (platform);
