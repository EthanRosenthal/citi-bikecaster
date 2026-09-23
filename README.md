# citi-bikecaster

Logs the [Citi Bike GBFS](https://gbfs.citibikenyc.com/gbfs/gbfs.json) station
status feed every 2 minutes (since 2016) to S3 as parquet that DuckDB and
Athena read directly.

## Data

Everything lives under `s3://insulator-citi-bikecaster/v2/`:

| Path | Contents |
| --- | --- |
| `station_status/month=YYYY-MM/YYYY-MM.parquet` | One file per closed month, sorted by `station_id, fetched_at` |
| `station_status/month=YYYY-MM/YYYY-MM-DD.parquet` | Daily files for the current month (folded into the monthly file on the 2nd) |
| `raw/station_status/date=YYYY-MM-DD/*.parquet` | Raw 2-minute snapshots, expire after 14 days (today's data lives only here) |
| `station_info/date=YYYY-MM-DD/station_info.parquet` | Daily snapshot of station names/locations/capacity |
| `dumps/<timestamp>/year=YYYY/*` | On-demand dumps (see below) |

`station_status` columns: `fetched_at`, `feed_last_updated`, `station_id`,
`legacy_id`, `num_bikes_available`, `num_ebikes_available`,
`num_bikes_disabled`, `num_docks_available`, `num_docks_disabled`,
`is_installed`, `is_renting`, `is_returning`, `last_reported`, `source`.
Timestamps are UTC.

`source` records where each row came from. `live` rows were written by v2.
`legacy_raw` rows were rebuilt from the original 2-minute files, so their
`fetched_at` is exact. `legacy_hourly` rows were rebuilt from hourly files,
which covers 2016-09 to 2019-08-01, and their `fetched_at` is only accurate to
the hour. Older rows use numeric `station_id`s. The feed switched to UUIDs
sometime between mid-2021 and 2023. `legacy_id`, populated on `live` rows only,
maps a UUID back to its numeric id.

### Data quality notes

- From 2023-08-05 to 2023-09-07 the feed listed most stations twice per
  snapshot. Exact duplicate rows are dropped, 49.1M rows in total, including
  a few short bursts in 2019–2021. About 1.5% of those pairs differ in some
  value, and both rows are kept.
- 2023-08-04 comes from hourly files, where duplicates can't be told apart
  from real repeated snapshots, so it still contains the duplicated stations.
- Seven days whose raw snapshots are incomplete fall back to hourly
  `fetched_at`: 2019-08-01, 2019-10-05, 2020-11-25, 2021-12-07, 2023-06-13,
  2023-08-04 and 2025-10-20.
- Some months have gaps in the source data. 2017-07 is nearly empty and
  2017-08 has no data.
- `last_reported` values before 2001 are GBFS "never" sentinels and are null.

### DuckDB

```sql
CREATE SECRET (TYPE s3, PROVIDER credential_chain, PROFILE 'rd', REGION 'us-east-1');
SET TimeZone = 'UTC';
CREATE VIEW status AS
  FROM read_parquet('s3://insulator-citi-bikecaster/v2/station_status/*/*.parquet', hive_partitioning = true);
SELECT month, count(*) FROM status GROUP BY ALL ORDER BY 1;
```

Filtering on `month` skips files entirely. Filtering on `station_id` or
`fetched_at` skips row groups.

### Athena

Database `citibike` in workgroup `citibike` has three tables:
`station_status`, `station_status_raw` and `station_info`. They use partition
projection, so partitions never need adding.

### Dumps

```sh
scripts/dump.sh athena        # Athena CTAS -> one zstd parquet file per year in S3
scripts/dump.sh duckdb x.parquet  # everything into a single local file
```

The Athena version runs the saved query `dump station_status`.

## Pipeline

All functions run on Python 3.14 (arm64) with a pyarrow layer. They are
defined in `template.yaml` (SAM) as stack `citibike-v2`.

| Function | Schedule (UTC) | Does |
| --- | --- | --- |
| `citibike-station-status` | every 2 min | fetch feed -> one raw parquet file |
| `citibike-station-info` | 00:05 daily | fetch station info |
| `citibike-compact-daily` | 00:30 daily | yesterday's raw files -> one sorted daily file |
| `citibike-compact-monthly` | 03:00 on the 2nd | last month's daily files -> one monthly file |
| `citibike-backfill` | manual | rebuild legacy data (see `scripts/backfill.py`) |

The compaction functions accept overrides for re-runs:
`make invoke STAGE=prod FN=compact-daily EVENT='{"date": "2026-09-22"}'` and
`FN=compact-monthly EVENT='{"month": "2026-09"}'`.

## Development

```sh
uv sync
make test                 # unit tests (moto S3, recorded GBFS + legacy fixtures)
make test-live            # smoke test against the real feed
make deploy STAGE=dev     # stack citibike-v2-dev: s3://.../dev/v2/, db citibike_dev
make deploy STAGE=prod    # stack citibike-v2
make invoke STAGE=dev FN=station-status
make lifecycle            # apply infra/lifecycle.json (bucket isn't owned by the stack)
```

Deploys need only the AWS CLI and uv; there is no Node or Serverless
dependency. The stacks reference the existing bucket and never create or
delete it.
