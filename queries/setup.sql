-- DuckDB session setup for the citi-bikecaster data.
--   make duckdb
--   (or: uvx --from duckdb-cli duckdb -init queries/setup.sql)
-- Uses your AWS credentials (profile rd; override with AWS_PROFILE).

CREATE OR REPLACE SECRET citibike (
    TYPE s3, PROVIDER credential_chain, PROFILE 'rd', REGION 'us-east-1'
);

-- Timestamps are stored in UTC; show them in NYC time. Note that the `month`
-- partition is a UTC month.
SET TimeZone = 'America/New_York';

-- Every 2-minute snapshot since 2016-09, through yesterday.
CREATE OR REPLACE VIEW status AS
FROM read_parquet(
    's3://insulator-citi-bikecaster/v2/station_status/*/*.parquet',
    hive_partitioning = true
);

-- Raw snapshots from the last ~14 days, including today (not yet compacted).
CREATE OR REPLACE VIEW recent AS
FROM read_parquet(
    's3://insulator-citi-bikecaster/v2/raw/station_status/*/*.parquet',
    hive_partitioning = true
);

-- Daily (weekly before 2026-09) snapshots of station metadata since 2019-08.
CREATE OR REPLACE VIEW station_info AS
FROM read_parquet(
    's3://insulator-citi-bikecaster/v2/station_info/*/*.parquet',
    hive_partitioning = true
);

-- One row per station_id with its most recent name/location/capacity. Covers
-- both the old numeric ids and the newer UUIDs.
CREATE OR REPLACE TABLE stations AS
SELECT
    station_id,
    arg_max(name, fetched_at) AS name,
    arg_max(lat, fetched_at) AS lat,
    arg_max(lon, fetched_at) AS lon,
    arg_max(capacity, fetched_at) AS capacity,
    min(fetched_at) AS first_seen,
    max(fetched_at) AS last_seen
FROM station_info
GROUP BY station_id;

-- find_station('grand army') -> matching stations, most recently seen first.
CREATE OR REPLACE MACRO find_station(q) AS TABLE
    SELECT * FROM stations WHERE name ILIKE '%' || q || '%' ORDER BY last_seen DESC;
