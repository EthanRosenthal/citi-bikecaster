from datetime import UTC, date, datetime, timedelta

import duckdb
import pyarrow.parquet as pq
import pytest

from bikecaster import common, compact, ingest

from conftest import keys


def write_raw_day(gbfs_status, day: date, snapshots: int = 3) -> int:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    rows = 0
    for i in range(snapshots):
        fetched = start + timedelta(minutes=2 * i, seconds=22)
        table = ingest.status_table(gbfs_status, fetched)
        common.write_parquet(table, common.raw_status_key(fetched, f"id{i}"))
        rows += table.num_rows
    return rows


def assert_sorted(table):
    assert table.equals(common.sort_status(table))


def test_compact_day(aws, gbfs_status):
    day = date(2026, 9, 1)
    rows = write_raw_day(gbfs_status, day)
    result = compact.daily({"date": "2026-09-01"}, None)
    assert result == {
        "key": common.daily_status_key(day),
        "rows": rows,
        "files": 3,
        "snapshots": 3,
    }
    table = common.read_parquet(result["key"])
    assert table.schema == common.STATUS_SCHEMA
    assert_sorted(table)

    # Idempotent re-run.
    assert compact.daily({"date": "2026-09-01"}, None)["rows"] == rows
    assert keys(aws, common.status_month_prefix("2026-09")) == [result["key"]]


def test_compact_day_without_raw_fails(aws):
    with pytest.raises(RuntimeError, match="No raw files"):
        compact.compact_day(date(2026, 9, 1))


def test_consolidate_month(aws, gbfs_status, tmp_path):
    month = "2026-09"
    expected = 0
    for d in (1, 2):
        expected += write_raw_day(gbfs_status, date(2026, 9, d))
        compact.compact_day(date(2026, 9, d))

    result = compact.monthly({"month": month}, None)
    assert result["rows"] == expected
    assert result["consolidated"] == 2
    monthly_key = common.monthly_status_key(month)
    assert keys(aws, common.status_month_prefix(month)) == [monthly_key]
    head = common.head(monthly_key)
    assert head["Metadata"]["days"] == "2026-09-01,2026-09-02"

    # The day is now sealed into the monthly file.
    with pytest.raises(RuntimeError, match="already part"):
        compact.compact_day(date(2026, 9, 1))

    # A late day gets merged in with the existing monthly rows.
    expected += write_raw_day(gbfs_status, date(2026, 9, 3))
    compact.compact_day(date(2026, 9, 3))
    result = compact.consolidate_month(month)
    assert result["rows"] == expected
    assert common.head(monthly_key)["Metadata"]["days"] == "2026-09-01,2026-09-02,2026-09-03"

    # Leftover daily from an interrupted run is deleted, not double counted.
    common.write_parquet(common.read_parquet(monthly_key).slice(0, 10), common.daily_status_key(date(2026, 9, 2)))
    result = compact.consolidate_month(month)
    assert result == {"month": month, "consolidated": 0, "stale_removed": 1}
    assert keys(aws, common.status_month_prefix(month)) == [monthly_key]

    # DuckDB reads it with hive partitioning and proper types.
    local = tmp_path / "month=2026-09" / "2026-09.parquet"
    local.parent.mkdir()
    common.download_file(monthly_key, str(local))
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    types = dict(
        con.execute(
            f"SELECT column_name, column_type FROM (DESCRIBE FROM read_parquet('{tmp_path}/*/*.parquet', hive_partitioning=true))"
        ).fetchall()
    )
    assert types["fetched_at"] == "TIMESTAMP WITH TIME ZONE"
    assert types["is_renting"] == "BOOLEAN"
    assert types["month"] == "VARCHAR"
    n, snaps = con.execute(
        f"SELECT count(*), count(DISTINCT fetched_at) FROM read_parquet('{tmp_path}/*/*.parquet')"
    ).fetchone()
    assert (n, snaps) == (expected, 9)
    assert pq.read_metadata(local).row_group(0).column(0).compression == "ZSTD"


def test_monthly_defaults_to_previous_month(aws, monkeypatch):
    seen = []
    monkeypatch.setattr(compact, "consolidate_month", seen.append)
    compact.monthly({"time": "2026-10-02T03:00:00Z"}, None)
    assert seen == [common.previous_month(datetime.now(UTC).date())]


def test_compact_day_drops_exact_duplicates(aws, gbfs_status):
    day = date(2026, 9, 1)
    rows = write_raw_day(gbfs_status, day)
    fetched = datetime(2026, 9, 1, tzinfo=UTC) + timedelta(seconds=22)
    table = ingest.status_table(gbfs_status, fetched)
    common.write_parquet(table, common.raw_status_key(fetched, "id0-retry"))
    result = compact.compact_day(day)
    assert result["rows"] == rows
    assert_sorted(common.read_parquet(result["key"]))
