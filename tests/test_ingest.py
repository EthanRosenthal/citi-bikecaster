from datetime import UTC, datetime

import pyarrow as pa
import pytest

from bikecaster import common, ingest

FETCHED = datetime(2026, 9, 22, 12, 0, 22, tzinfo=UTC)


def test_status_table(gbfs_status):
    table = ingest.status_table(gbfs_status, FETCHED)
    assert table.schema == common.STATUS_SCHEMA
    assert table.num_rows == 50
    row = table.slice(0, 1).to_pylist()[0]
    assert row["fetched_at"] == FETCHED
    assert row["source"] == "live"
    assert isinstance(row["legacy_id"], str)
    # 86400 is GBFS's "never reported" sentinel.
    assert row["last_reported"] is None
    assert table["is_installed"].type == pa.bool_()


def test_info_table(gbfs_info):
    table = ingest.info_table(gbfs_info, FETCHED)
    assert table.schema == common.INFO_SCHEMA
    assert table.num_rows == 50
    assert table["name"].null_count == 0


def test_station_status_handler(monkeypatch, gbfs_status):
    monkeypatch.setattr(ingest, "fetch_json", lambda url: gbfs_status)
    result = ingest.station_status({}, None)
    assert result["key"].startswith(f"{common.PREFIX}/raw/station_status/date=")
    assert common.read_parquet(result["key"]).schema == common.STATUS_SCHEMA


def test_station_info_handler(monkeypatch, gbfs_info):
    monkeypatch.setattr(ingest, "fetch_json", lambda url: gbfs_info)
    result = ingest.station_info({}, None)
    assert "/station_info/date=" in result["key"]
    assert common.read_parquet(result["key"]).num_rows == 50


@pytest.mark.live
def test_live_feeds():
    now = datetime.now(UTC)
    status = ingest.status_table(ingest.fetch_json(ingest.STATUS_URL), now)
    info = ingest.info_table(ingest.fetch_json(ingest.INFO_URL), now)
    assert status.num_rows > 1000
    assert info.num_rows > 1000
    assert status["station_id"].null_count == 0
