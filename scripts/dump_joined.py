"""Dump station_status joined to the station_info in effect at each poll.

Each station_status row gets the most recent station_info snapshot for its
station taken at or before its fetched_at (a DuckDB ASOF JOIN). The columns
match the Kaggle "Citi Bike Stations" dataset
(kaggle.com/datasets/rosenthal/citi-bike-stations), with native timestamps
and a few extra columns appended. Output is one zstd parquet file per year,
sorted by station_id and fetched_at.

    uv run scripts/dump_joined.py OUT_DIR [--years 2016 2017 ...] [--threads N]

Station info is only available from 2019-08. Earlier rows (and stations that
never appear in station_info) have missing_station_information = true.
"""

import argparse
from datetime import UTC, datetime
from pathlib import Path
import time

import duckdb

BASE = "s3://insulator-citi-bikecaster/v2"

QUERY = """
COPY (
    SELECT
        s.station_id,
        s.num_bikes_available,
        s.num_ebikes_available,
        s.num_bikes_disabled,
        s.num_docks_available,
        s.num_docks_disabled,
        s.is_installed,
        s.is_renting,
        s.is_returning,
        s.last_reported AS station_status_last_reported,
        i.name AS station_name,
        i.lat,
        i.lon,
        i.region_id,
        i.capacity,
        i.has_kiosk,
        i.feed_last_updated AS station_information_last_updated,
        i.station_id IS NULL AS missing_station_information,
        -- Not in the 2021 Kaggle version:
        s.fetched_at,
        s.source,
        s.legacy_id,
        i.fetched_at AS station_information_fetched_at
    FROM (
        FROM read_parquet('{base}/station_status/*/*.parquet', hive_partitioning = true)
        WHERE month LIKE '{year}-%'
    ) s
    ASOF LEFT JOIN station_info i
        ON s.station_id = i.station_id AND s.fetched_at >= i.fetched_at
    ORDER BY s.station_id, s.fetched_at
) TO '{out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("out_dir", type=Path)
    parser.add_argument("--years", nargs="*", type=int)
    parser.add_argument("--profile", default="rd")
    parser.add_argument("--threads", type=int)
    parser.add_argument("--memory-limit", default="20GB")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    years = args.years or range(2016, datetime.now(UTC).year + 1)

    con = duckdb.connect()
    con.sql(
        f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, PROFILE '{args.profile}', REGION 'us-east-1')"
    )
    con.sql("SET TimeZone = 'UTC'")
    con.sql(f"SET memory_limit = '{args.memory_limit}'")
    con.sql(f"SET temp_directory = '{args.out_dir / '.tmp'}'")
    con.sql("SET preserve_insertion_order = false")
    if args.threads:
        con.sql(f"SET threads = {args.threads}")
    # Small (~700k rows): load once.
    con.sql(
        f"CREATE TABLE station_info AS "
        f"FROM read_parquet('{BASE}/station_info/*/*.parquet', hive_partitioning = true)"
    )

    for year in years:
        out = args.out_dir / f"citi_bike_stations_{year}.parquet"
        t0 = time.time()
        con.sql(QUERY.format(base=BASE, year=year, out=out))
        rows = con.sql(f"SELECT count(*) FROM '{out}'").fetchone()[0]
        size = out.stat().st_size / 1e9
        print(f"{out.name}: {rows:,} rows, {size:.2f} GB in {time.time() - t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
