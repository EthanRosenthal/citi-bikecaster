from datetime import UTC, date, datetime, timedelta
import io

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from bikecaster import backfill, common

from conftest import DATA, keys, put_file

DAY = date(2021, 6, 1)
HOURLY = DATA / "station_status_2021_06_01_00.parquet"


def upload_consistent_day(client, drop_snapshots: int = 0) -> int:
    """Upload the 2021-06-01 00h hourly file plus the 30 raw snapshots it was
    concatenated from (split on the pandas index resets), like the old
    file_concatenater left behind in trash/."""
    put_file(client, HOURLY, f"station_status/2021/06/01/00/{HOURLY.name}")
    table = pq.read_table(HOURLY)
    starts = [i for i, v in enumerate(table["index"].to_pylist()) if v == 0] + [table.num_rows]
    for n, (a, b) in enumerate(zip(starts, starts[1:])):
        if n < drop_snapshots:
            continue
        ts = datetime(2021, 6, 1, 0, tzinfo=UTC) + timedelta(minutes=2 * n, seconds=22)
        name = f"juvenai-1-{ts:%Y-%m-%d-%H-%M-%S}-{n:04d}.parquet"
        buf = io.BytesIO()
        pq.write_table(table.slice(a, b - a).drop_columns(["index"]), buf)
        client.put_object(
            Bucket=common.BUCKET, Key=f"trash/station_status/2021/06/01/00/{name}", Body=buf.getvalue()
        )
    return table.num_rows


def test_auto_prefers_raw_when_counts_match(aws):
    rows = upload_consistent_day(aws)
    result = backfill.handler({"month": "2021-06", "days": ["2021-06-01"]}, None)
    assert result["rows"] == rows
    (info,) = result["per_day"]
    assert info["chosen"] == "legacy_raw"
    assert info["raw_files"] == 30
    assert "flag" not in info

    table = common.read_parquet(common.monthly_status_key("2021-06"))
    assert table.schema == common.STATUS_SCHEMA
    assert pc.count_distinct(table["fetched_at"]).as_py() == 30
    assert table.equals(common.sort_status(table))
    assert common.head(common.monthly_status_key("2021-06"))["Metadata"]["days"] == "2021-06-01"


def test_auto_falls_back_to_hourly_on_mismatch(aws):
    rows = upload_consistent_day(aws, drop_snapshots=3)
    result = backfill.handler({"month": "2021-06", "days": ["2021-06-01"], "dry_run": True}, None)
    (info,) = result["per_day"]
    assert info["chosen"] == "legacy_hourly"
    assert info["flag"]
    assert info["rows"] == rows
    assert keys(aws, "v2/") == []


def test_raw_drops_concatenated_files(aws):
    upload_consistent_day(aws)
    # An hourly concatenated file that ended up in trash.
    put_file(aws, HOURLY, "trash/station_status/2021/06/01/00/juvenai-1-2021-06-01-00-00-00-big.parquet")
    table, info = backfill.raw_day(DAY)
    assert info["raw_dropped"] == 1
    assert info["raw_rows"] == pq.read_metadata(HOURLY).num_rows


def test_hourly_source_uses_hour_as_fetched_at(aws):
    path = DATA / "station_status_2016_09_20_12.parquet"
    put_file(aws, path, f"station_status/2016/09/20/12/{path.name}")
    result = backfill.handler(
        {"month": "2016-09", "source": "legacy_hourly", "output": "daily", "days": ["2016-09-20"]}, None
    )
    assert result["rows"] == pq.read_metadata(path).num_rows
    table = common.read_parquet(common.daily_status_key(date(2016, 9, 20)))
    assert table["fetched_at"].unique().to_pylist() == [datetime(2016, 9, 20, 12, tzinfo=UTC)]
    assert table["source"].unique().to_pylist() == ["legacy_hourly"]


def test_refuses_to_overwrite_v2_month(aws):
    upload_consistent_day(aws)
    backfill.handler({"month": "2021-06", "days": ["2021-06-01"]}, None)
    with pytest.raises(RuntimeError, match="already has v2 data"):
        backfill.handler({"month": "2021-06", "days": ["2021-06-01"]}, None)
    with pytest.raises(RuntimeError, match="already in monthly"):
        backfill.handler({"month": "2021-06", "days": ["2021-06-01"], "output": "daily"}, None)
