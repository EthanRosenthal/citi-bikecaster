from datetime import UTC, date, datetime

import pyarrow.parquet as pq
import pytest

from bikecaster import common

from conftest import DATA, put_file

LEGACY_HOURLY = sorted(DATA.glob("station_status_*.parquet"))
LEGACY_RAW = sorted(DATA.glob("juvenai-*.parquet"))


@pytest.mark.parametrize("path", LEGACY_HOURLY + LEGACY_RAW, ids=lambda p: p.name[:30])
def test_normalize_legacy(path):
    raw = pq.read_table(path)
    hour = datetime(2020, 1, 1, tzinfo=UTC)
    table = common.normalize_status(raw, "legacy_hourly", fetched_at=hour)
    assert table.schema == common.STATUS_SCHEMA
    assert table.num_rows == raw.num_rows
    assert table["station_id"].null_count == 0
    assert table["fetched_at"].null_count == 0
    assert table["feed_last_updated"].null_count == table.num_rows
    assert table["num_bikes_available"].to_pylist() == raw["num_bikes_available"].to_pylist()
    if "num_ebikes_available" not in raw.column_names:
        assert table["num_ebikes_available"].null_count == table.num_rows
    # Sentinel last_reported values (e.g. -18000, 86400) become null, real ones survive.
    valid = [v for v in raw["last_reported"].to_pylist() if v >= common.MIN_VALID_EPOCH]
    assert table["last_reported"].null_count == table.num_rows - len(valid)


def test_fetched_at_from_legacy_name():
    key = "trash/station_status/2023/05/05/05/juvenai-1-2023-05-05-05-00-22-71aa.parquet"
    assert common.fetched_at_from_legacy_name(key) == datetime(2023, 5, 5, 5, 0, 22, tzinfo=UTC)


def test_parquet_num_rows(aws):
    for path in LEGACY_HOURLY:
        put_file(aws, path, f"x/{path.name}")
        assert common.parquet_num_rows(f"x/{path.name}") == pq.read_metadata(path).num_rows


def test_keys():
    d = date(2026, 9, 2)
    assert common.daily_status_key(d) == "v2/station_status/month=2026-09/2026-09-02.parquet"
    assert common.monthly_status_key("2026-09") == "v2/station_status/month=2026-09/2026-09.parquet"
    assert common.raw_status_prefix(d) == "v2/raw/station_status/date=2026-09-02/"
    assert common.previous_month(date(2026, 1, 2)) == "2025-12"
    assert len(common.month_days("2024-02")) == 29
