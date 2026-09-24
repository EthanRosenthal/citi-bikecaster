-- Example queries. Run `make duckdb`, then paste any of these.
-- Filtering on `month` (a UTC 'YYYY-MM' string) skips whole files. Queries
-- that span all history read a few GB from S3.

-- 1. What's in there? Row counts per month (reads only file footers, ~3 s).
SELECT month, count(*) AS rows
FROM status
GROUP BY ALL
ORDER BY month;

-- 2. Find a station. Old numeric ids and newer UUIDs both show up, since
--    stations were re-keyed in early 2023.
FROM find_station('grand army');

-- 3. Right now: bikes and docks at stations matching a name. Reads just the
--    newest raw snapshot (re-run the SET to refresh).
SET VARIABLE latest = (
    SELECT max(file) FROM glob('s3://insulator-citi-bikecaster/v2/raw/station_status/*/*.parquet')
);
SELECT s.name, r.fetched_at, r.num_bikes_available, r.num_ebikes_available, r.num_docks_available
FROM read_parquet(getvariable('latest')) r
JOIN stations s USING (station_id)
WHERE s.name ILIKE '%grand army%'
ORDER BY s.name;

-- 4. A typical day at one station: average bikes available by hour (NYC time)
--    over a month.
SELECT hour(fetched_at) AS hour_nyc,
       round(avg(num_bikes_available), 1) AS avg_bikes,
       round(avg(num_ebikes_available), 1) AS avg_ebikes,
       bar(avg(num_bikes_available), 0, 60, 30) AS chart
FROM status
WHERE month = '2026-08'
  AND station_id IN (SELECT station_id FROM find_station('W 21 St & 6 Ave'))
GROUP BY ALL
ORDER BY hour_nyc;

-- 5. Stations most often completely empty last month (share of snapshots
--    with zero bikes while renting).
SELECT s.name,
       round(100 * avg((num_bikes_available = 0)::int), 1) AS pct_empty,
       count(*) AS snapshots
FROM status
JOIN stations s USING (station_id)
WHERE month = '2026-08' AND is_renting
GROUP BY ALL
HAVING count(*) > 10000
ORDER BY pct_empty DESC
LIMIT 15;

-- 6. System-wide bikes over one month: total docked bikes (and e-bikes) at
--    each snapshot, averaged per NYC day. `month` is a UTC month, so the
--    first row is the evening of the previous day.
SELECT fetched_at::date AS day,
       round(avg(total_bikes)) AS avg_bikes_docked,
       round(avg(total_ebikes)) AS avg_ebikes_docked
FROM (
    SELECT fetched_at,
           sum(num_bikes_available) AS total_bikes,
           sum(num_ebikes_available) AS total_ebikes
    FROM status
    WHERE month = '2026-08'
    GROUP BY fetched_at
)
GROUP BY ALL
ORDER BY day;

-- 7. The network growing over the years: stations and docks in the last
--    station_info snapshot of each year.
WITH last_snapshot AS (
    SELECT max(fetched_at) AS fetched_at FROM station_info GROUP BY year(fetched_at)
)
SELECT year(fetched_at) AS year, count(*) AS stations, sum(capacity) AS docks
FROM station_info
JOIN last_snapshot USING (fetched_at)
GROUP BY ALL
ORDER BY year;

-- 8. Export one station's full history (2016 to yesterday) to CSV. This
--    reads every month, so it takes a minute or two.
COPY (
    SELECT fetched_at, num_bikes_available, num_ebikes_available, num_docks_available
    FROM status
    WHERE station_id IN (SELECT station_id FROM find_station('W 21 St & 6 Ave'))
    ORDER BY fetched_at
) TO 'w21_6ave.csv';

-- 9. Download everything into one local parquet file (~9 GB) for offline use.
-- COPY status TO 'citibike_station_status.parquet' (FORMAT parquet, COMPRESSION zstd);
