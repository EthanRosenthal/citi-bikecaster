"""Compaction: raw 2-minute snapshots -> daily files -> one file per month."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
import os
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from bikecaster import common
from bikecaster.common import logger

ZSTD_LEVEL = 9
ROW_GROUP_SIZE = 1_000_000
READ_THREADS = 64


def read_many(keys: list[str], threads: int = READ_THREADS) -> list[pa.Table]:
    with ThreadPoolExecutor(threads) as pool:
        return list(pool.map(common.read_parquet, keys))


def _days_in(head: dict | None) -> set[str]:
    if head is None:
        return set()
    return {d for d in head.get("Metadata", {}).get("days", "").split(",") if d}


class MonthWriter:
    """Streams sorted per-day tables into a single local parquet file, then
    uploads it as the month's file with the covered days in S3 metadata.

    S3 PUTs are atomic, so readers see either the old or the new monthly file.
    """

    def __init__(self, month: str):
        self.month = month
        self.days: set[str] = set()
        self.rows = 0
        self._dir = tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))
        self.path = os.path.join(self._dir.name, f"{month}.parquet")
        self._writer = pq.ParquetWriter(
            self.path,
            common.STATUS_SCHEMA,
            compression="zstd",
            compression_level=ZSTD_LEVEL,
        )

    def write(self, table: pa.Table, days: set[str]) -> None:
        self._writer.write_table(table.cast(common.STATUS_SCHEMA), row_group_size=ROW_GROUP_SIZE)
        self.rows += table.num_rows
        self.days |= days

    def upload(self, expected_rows: int) -> dict:
        self._writer.close()
        written = pq.read_metadata(self.path).num_rows
        if written != expected_rows or written != self.rows:
            raise RuntimeError(
                f"{self.month}: wrote {written} rows, expected {expected_rows} ({self.rows} buffered)"
            )
        key = common.monthly_status_key(self.month)
        size = os.path.getsize(self.path)
        common.upload_file(
            self.path, key, metadata={"days": ",".join(sorted(self.days)), "rows": str(written)}
        )
        self._dir.cleanup()
        logger.info("Uploaded %s: %d rows, %d days, %.1f MB", key, written, len(self.days), size / 1e6)
        return {"key": key, "rows": written, "days": len(self.days), "bytes": size}

    def close(self) -> None:
        self._writer.close()
        self._dir.cleanup()


# ---------------------------------------------------------------------------
# Daily
# ---------------------------------------------------------------------------


def compact_day(day: date) -> dict:
    """Merge one day's raw snapshots into a single sorted daily file.

    Idempotent: re-running overwrites the daily file. Raw files are left in
    place and expire via the bucket lifecycle rule.
    """
    month = f"{day:%Y-%m}"
    if day.isoformat() in _days_in(common.head(common.monthly_status_key(month))):
        raise RuntimeError(f"{day} is already part of the {month} monthly file")

    keys = [o["Key"] for o in common.list_objects(common.raw_status_prefix(day))]
    if not keys:
        raise RuntimeError(f"No raw files found for {day}")

    table = pa.concat_tables(read_many(keys)).cast(common.STATUS_SCHEMA)
    total = table.num_rows
    table = common.sort_status(common.drop_exact_duplicates(table))
    if table.num_rows != total:
        logger.warning("%s: dropped %d exact duplicate rows", day, total - table.num_rows)
    key = common.daily_status_key(day)
    common.write_parquet(
        table, key, compression_level=ZSTD_LEVEL, row_group_size=ROW_GROUP_SIZE
    )
    snapshots = pc.count_distinct(table["fetched_at"]).as_py()
    if snapshots < 700:
        logger.warning("%s: only %d snapshots (expected ~720)", day, snapshots)
    logger.info("Wrote %s: %d rows from %d raw files", key, table.num_rows, len(keys))
    return {"key": key, "rows": table.num_rows, "files": len(keys), "snapshots": snapshots}


def daily(event, context):
    """Scheduled after midnight UTC. Event may override {"date": "YYYY-MM-DD"}."""
    event = event or {}
    if "date" in event:
        day = date.fromisoformat(event["date"])
    else:
        day = datetime.now(UTC).date() - timedelta(days=1)
    return compact_day(day)


# ---------------------------------------------------------------------------
# Monthly
# ---------------------------------------------------------------------------


def _day_of(key: str) -> str:
    return key.rsplit("/", 1)[-1].removesuffix(".parquet")


def consolidate_month(month: str) -> dict:
    """Fold all daily files in a month partition into the single monthly file.

    If a monthly file already exists (e.g. a late backfill added days), its
    rows are carried over. Daily files whose day is already recorded in the
    monthly file are leftovers from an interrupted run and are just deleted.
    """
    monthly_key = common.monthly_status_key(month)
    dailies = sorted(
        o["Key"]
        for o in common.list_objects(common.status_month_prefix(month))
        if o["Key"] != monthly_key and o["Key"].endswith(".parquet")
    )
    existing = common.head(monthly_key)
    existing_days = _days_in(existing)
    stale = [k for k in dailies if _day_of(k) in existing_days]
    fresh = [k for k in dailies if _day_of(k) not in existing_days]

    if not fresh:
        common.delete_keys(stale)
        logger.info("%s: nothing to consolidate (%d stale dailies removed)", month, len(stale))
        return {"month": month, "consolidated": 0, "stale_removed": len(stale)}

    writer = MonthWriter(month)
    try:
        expected = 0
        if existing:
            local = os.path.join(writer._dir.name, "existing.parquet")
            common.download_file(monthly_key, local)
            pf = pq.ParquetFile(local)
            for i in range(pf.num_row_groups):
                writer.write(pf.read_row_group(i), set())
            writer.days |= existing_days
            expected += pf.metadata.num_rows
        for key in fresh:
            table = common.read_parquet(key)
            writer.write(table, {_day_of(key)})
            expected += table.num_rows
        result = writer.upload(expected)
    except BaseException:
        writer.close()
        raise

    common.delete_keys(fresh + stale)
    return {**result, "month": month, "consolidated": len(fresh), "stale_removed": len(stale)}


def monthly(event, context):
    """Scheduled early on the 2nd. Event may override {"month": "YYYY-MM"}."""
    event = event or {}
    month = event.get("month") or common.previous_month(datetime.now(UTC).date())
    return consolidate_month(month)
