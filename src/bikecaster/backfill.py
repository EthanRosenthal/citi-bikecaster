"""Rebuild legacy (pre-v2) station_status data into the v2 layout.

Legacy sources in the bucket:

* ``trash/station_status/YYYY/MM/DD/HH/juvenai-1-<timestamp>-<uuid>.parquet``:
  the original 2-minute snapshots (2019-08 onward). The filename carries the
  exact fetch time -> ``source = legacy_raw``.
* ``station_status/YYYY/MM/DD/HH/*.parquet``: hourly files (all history).
  Only the hour is known -> ``source = legacy_hourly``.

With ``source = "auto"`` each day uses the raw snapshots when their row count
matches the hourly files (within ``TOLERANCE``), otherwise it falls back to the
hourly files and flags the day in the result.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
import statistics

import pyarrow as pa

from bikecaster import common
from bikecaster.common import logger
from bikecaster.compact import ROW_GROUP_SIZE, ZSTD_LEVEL, MonthWriter, _days_in, read_many

TOLERANCE = 0.005
RAW_PREFIX = "trash/station_status"
HOURLY_PREFIX = "station_status"


def _day_path(day: date) -> str:
    return f"{day:%Y/%m/%d}/"


def raw_day(day: date) -> tuple[pa.Table | None, dict]:
    keys = sorted(
        o["Key"]
        for o in common.list_objects(f"{RAW_PREFIX}/{_day_path(day)}")
        if o["Key"].endswith(".parquet")
    )
    if not keys:
        return None, {"raw_files": 0, "raw_rows": 0}
    tables = read_many(keys)
    # Re-runs of the old concatenater could move a whole hourly file into
    # trash; those would duplicate snapshots, so drop anything far larger
    # than a single snapshot.
    median = statistics.median(t.num_rows for t in tables)
    kept, dropped = [], 0
    for key, table in zip(keys, tables):
        if table.num_rows > 2 * median:
            dropped += 1
            continue
        kept.append(
            common.normalize_status(
                table, "legacy_raw", fetched_at=common.fetched_at_from_legacy_name(key)
            )
        )
    table = pa.concat_tables(kept)
    return table, {"raw_files": len(keys), "raw_dropped": dropped, "raw_rows": table.num_rows}


def hourly_keys(day: date) -> list[str]:
    return sorted(
        o["Key"]
        for o in common.list_objects(f"{HOURLY_PREFIX}/{_day_path(day)}")
        if o["Key"].endswith(".parquet")
    )


def _hour_of(key: str) -> datetime:
    y, m, d, h = key.split("/")[1:5]
    return datetime(int(y), int(m), int(d), int(h), tzinfo=UTC)


def hourly_day(keys: list[str]) -> pa.Table | None:
    if not keys:
        return None
    tables = read_many(keys)
    return pa.concat_tables(
        common.normalize_status(t, "legacy_hourly", fetched_at=_hour_of(k))
        for k, t in zip(keys, tables)
    )


def build_day(day: date, source: str) -> tuple[pa.Table | None, dict]:
    info: dict = {"day": day.isoformat()}
    if source == "legacy_hourly":
        table = hourly_day(hourly_keys(day))
        info["chosen"] = "legacy_hourly" if table is not None else None
    elif source == "legacy_raw":
        table, raw_info = raw_day(day)
        info |= raw_info
        info["chosen"] = "legacy_raw" if table is not None else None
    elif source == "auto":
        keys = hourly_keys(day)
        with ThreadPoolExecutor(24) as pool:
            hourly_rows = sum(pool.map(common.parquet_num_rows, keys))
        table, raw_info = raw_day(day)
        info |= raw_info | {"hourly_rows": hourly_rows}
        raw_rows = raw_info["raw_rows"]
        if table is not None and (
            hourly_rows == 0 or abs(raw_rows - hourly_rows) <= TOLERANCE * hourly_rows
        ):
            info["chosen"] = "legacy_raw"
        else:
            table = hourly_day(keys)
            info["chosen"] = "legacy_hourly" if table is not None else None
            if table is not None and raw_rows:
                info["flag"] = "raw/hourly row count mismatch"
    else:
        raise ValueError(f"Unknown source {source!r}")

    if table is None:
        info["rows"] = 0
        return None, info
    table = common.sort_status(table)
    info["rows"] = table.num_rows
    return table, info


def backfill(month: str, source: str = "auto", days: list[str] | None = None,
             output: str = "monthly", dry_run: bool = False) -> dict:
    wanted = [d for d in common.month_days(month) if days is None or d.isoformat() in days]
    monthly_key = common.monthly_status_key(month)
    existing = common.head(monthly_key)

    if not dry_run:
        if output == "monthly" and (existing or common.list_objects(common.status_month_prefix(month))):
            raise RuntimeError(f"{month} already has v2 data; use output='daily' for specific days")
        if output == "daily":
            clash = {d.isoformat() for d in wanted} & _days_in(existing)
            if clash:
                raise RuntimeError(f"Days already in monthly file: {sorted(clash)}")

    report = []
    writer = MonthWriter(month) if output == "monthly" and not dry_run else None
    try:
        for day in wanted:
            table, info = build_day(day, source)
            report.append(info)
            logger.info("backfill %s", info)
            if table is None or dry_run:
                continue
            if writer:
                writer.write(table, {day.isoformat()})
            else:
                common.write_parquet(table, common.daily_status_key(day),
                                     compression_level=ZSTD_LEVEL, row_group_size=ROW_GROUP_SIZE)
        result = writer.upload(sum(r["rows"] for r in report)) if writer and writer.rows else {}
    finally:
        if writer:
            writer.close()

    return {
        "month": month,
        "source": source,
        "output": output,
        "dry_run": dry_run,
        "rows": sum(r["rows"] for r in report),
        "per_day": report,
        **result,
    }


def handler(event, context):
    """Event: {"month": "YYYY-MM", "source": "auto", "days": [...], "output": "monthly", "dry_run": false}"""
    return backfill(
        event["month"],
        source=event.get("source", "auto"),
        days=event.get("days"),
        output=event.get("output", "monthly"),
        dry_run=event.get("dry_run", False),
    )
