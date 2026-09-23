"""Scheduled pollers for the Citi Bike GBFS feed."""

from datetime import UTC, datetime
import json
import time
import urllib.request
import uuid

import pyarrow as pa

from bikecaster import common
from bikecaster.common import logger

STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"


def fetch_json(url: str, attempts: int = 3, timeout: float = 20) -> dict:
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "citi-bikecaster"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.load(resp)
        except Exception:
            if attempt == attempts:
                raise
            logger.warning("Fetch of %s failed (attempt %d), retrying", url, attempt, exc_info=True)
            time.sleep(2 * attempt)


def status_table(payload: dict, fetched_at: datetime) -> pa.Table:
    stations = payload["data"]["stations"]
    cols = {
        name: [s.get(name) for s in stations]
        for name in common.STATUS_SCHEMA.names
        if name not in ("fetched_at", "feed_last_updated", "source", "last_reported")
    }
    cols["station_id"] = [str(s) for s in cols["station_id"]]
    cols["legacy_id"] = [None if s is None else str(s) for s in cols["legacy_id"]]
    for flag in ("is_installed", "is_renting", "is_returning"):
        cols[flag] = [None if v is None else bool(v) for v in cols[flag]]
    table = pa.table(cols).append_column(
        "last_reported", pa.array([s.get("last_reported") for s in stations], pa.int64())
    )
    return common.normalize_status(
        table,
        source="live",
        fetched_at=fetched_at,
        feed_last_updated=datetime.fromtimestamp(payload["last_updated"], UTC),
    )


def info_table(payload: dict, fetched_at: datetime) -> pa.Table:
    stations = payload["data"]["stations"]
    n = len(stations)
    cols = {
        "fetched_at": [fetched_at] * n,
        "feed_last_updated": [datetime.fromtimestamp(payload["last_updated"], UTC)] * n,
    }
    for name in common.INFO_SCHEMA.names[2:]:
        values = [s.get(name) for s in stations]
        if name in ("station_id", "short_name", "region_id"):
            values = [None if v is None else str(v) for v in values]
        cols[name] = values
    return pa.table(cols, schema=common.INFO_SCHEMA)


def station_status(event, context):
    fetched_at = datetime.now(UTC)
    table = status_table(fetch_json(STATUS_URL), fetched_at)
    key = common.raw_status_key(fetched_at, str(uuid.uuid4()))
    common.write_parquet(table, key)
    logger.info("Wrote %d rows to %s", table.num_rows, key)
    return {"rows": table.num_rows, "key": key}


def station_info(event, context):
    fetched_at = datetime.now(UTC)
    table = info_table(fetch_json(INFO_URL), fetched_at)
    key = common.info_key(fetched_at.date())
    common.write_parquet(table, key)
    logger.info("Wrote %d rows to %s", table.num_rows, key)
    return {"rows": table.num_rows, "key": key}
