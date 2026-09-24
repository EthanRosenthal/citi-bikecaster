"""Shared schema, S3 layout and I/O helpers.

S3 layout (all under ``s3://$BUCKET/$PREFIX/``)::

    raw/station_status/date=YYYY-MM-DD/HHMMSS-<uuid>.parquet   one file per poll
    station_status/month=YYYY-MM/YYYY-MM-DD.parquet             daily files (current month)
    station_status/month=YYYY-MM/YYYY-MM.parquet                one file per closed month
    station_info/date=YYYY-MM-DD/station_info.parquet           daily snapshot
"""

from datetime import UTC, date, datetime
import io
import logging
import os
import re

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ.get("BUCKET", "insulator-citi-bikecaster")
PREFIX = os.environ.get("PREFIX", "v2").strip("/")

TS = pa.timestamp("ms", tz="UTC")

STATUS_SCHEMA = pa.schema(
    [
        ("fetched_at", TS),
        ("feed_last_updated", TS),
        ("station_id", pa.string()),
        ("legacy_id", pa.string()),
        ("num_bikes_available", pa.int32()),
        ("num_ebikes_available", pa.int32()),
        ("num_bikes_disabled", pa.int32()),
        ("num_docks_available", pa.int32()),
        ("num_docks_disabled", pa.int32()),
        ("is_installed", pa.bool_()),
        ("is_renting", pa.bool_()),
        ("is_returning", pa.bool_()),
        ("last_reported", TS),
        # live | legacy_raw | legacy_hourly. legacy_hourly rows only know
        # fetched_at to the hour.
        ("source", pa.string()),
    ]
)
SORT_KEYS = [("station_id", "ascending"), ("fetched_at", "ascending")]

INFO_SCHEMA = pa.schema(
    [
        ("fetched_at", TS),
        ("feed_last_updated", TS),
        ("station_id", pa.string()),
        ("short_name", pa.string()),
        ("name", pa.string()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("region_id", pa.string()),
        ("capacity", pa.int32()),
        ("has_kiosk", pa.bool_()),
        ("station_type", pa.string()),
    ]
)

# GBFS uses tiny epoch values (e.g. 86400) as "never reported" sentinels.
MIN_VALID_EPOCH = 1_000_000_000


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------


def raw_status_prefix(day: date) -> str:
    return f"{PREFIX}/raw/station_status/date={day:%Y-%m-%d}/"


def raw_status_key(fetched_at: datetime, file_id: str) -> str:
    return f"{raw_status_prefix(fetched_at.date())}{fetched_at:%H%M%S}-{file_id}.parquet"


def status_month_prefix(month: str) -> str:
    return f"{PREFIX}/station_status/month={month}/"


def daily_status_key(day: date) -> str:
    return f"{status_month_prefix(f'{day:%Y-%m}')}{day:%Y-%m-%d}.parquet"


def monthly_status_key(month: str) -> str:
    return f"{status_month_prefix(month)}{month}.parquet"


def info_key(day: date) -> str:
    return f"{PREFIX}/station_info/date={day:%Y-%m-%d}/station_info.parquet"


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

_s3 = None


def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def list_objects(prefix: str) -> list[dict]:
    """All objects under ``prefix`` as dicts with Key and Size."""
    out = []
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        out.extend(page.get("Contents", []))
    return out


def exists(key: str) -> bool:
    return head(key) is not None


def head(key: str) -> dict | None:
    try:
        return s3().head_object(Bucket=BUCKET, Key=key)
    except s3().exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def read_parquet(key: str) -> pa.Table:
    body = s3().get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return pq.read_table(pa.BufferReader(body))


def parquet_num_rows(key: str) -> int:
    """Row count from the footer only (downloads just the tail of the file)."""
    size = s3().head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    tail = min(size, 64 * 1024)
    buf = s3().get_object(Bucket=BUCKET, Key=key, Range=f"bytes={size - tail}-")["Body"].read()
    footer_len = int.from_bytes(buf[-8:-4], "little")
    if footer_len + 8 > len(buf):
        return read_parquet(key).num_rows
    return pq.read_metadata(_FooterFile(buf, size)).num_rows


class _FooterFile(io.RawIOBase):
    """Seekable file-like exposing only the last bytes of a remote file."""

    def __init__(self, tail: bytes, size: int):
        self.tail, self.size, self.pos = tail, size, 0

    def seekable(self):
        return True

    def readable(self):
        return True

    def seek(self, offset, whence=0):
        self.pos = {0: offset, 1: self.pos + offset, 2: self.size + offset}[whence]
        return self.pos

    def tell(self):
        return self.pos

    def readinto(self, b):
        start = self.pos - (self.size - len(self.tail))
        if start < 0:
            raise OSError("read outside footer")
        chunk = self.tail[start : start + len(b)]
        b[: len(chunk)] = chunk
        self.pos += len(chunk)
        return len(chunk)


def write_parquet(table: pa.Table, key: str, **kwargs) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd", **kwargs)
    s3().put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())


def upload_file(path: str, key: str, metadata: dict | None = None) -> None:
    extra = {"Metadata": metadata} if metadata else None
    s3().upload_file(path, BUCKET, key, ExtraArgs=extra)


def download_file(key: str, path: str) -> None:
    s3().download_file(BUCKET, key, path)


def delete_keys(keys: list[str]) -> None:
    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i : i + 1000]]
        resp = s3().delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
        if resp.get("Errors"):
            raise RuntimeError(f"Failed to delete: {resp['Errors'][:5]}")


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def epoch_to_ts(arr: pa.Array | pa.ChunkedArray) -> pa.ChunkedArray:
    """Epoch seconds -> UTC timestamp, nulling out sentinel values."""
    if pa.types.is_timestamp(arr.type):
        return arr.cast(TS)
    secs = arr.cast(pa.int64())
    secs = pc.if_else(pc.less(secs, MIN_VALID_EPOCH), None, secs)
    return pc.multiply(secs, 1000).cast(pa.timestamp("ms")).cast(TS)


def normalize_status(
    table: pa.Table,
    source: str,
    fetched_at: datetime | None = None,
    feed_last_updated: datetime | None = None,
) -> pa.Table:
    """Coerce a station_status table from any era into STATUS_SCHEMA.

    Handles legacy quirks: the pandas ``index`` column, binary/int station
    ids, 0/1 integer flags, int64 counts and epoch-second timestamps. Columns
    missing from older eras (e.g. ebikes before 2019) become null.
    ``fetched_at`` / ``feed_last_updated`` fill those columns when absent.
    """
    n = table.num_rows
    cols = {}
    for field in STATUS_SCHEMA:
        name = field.name
        if name in table.column_names:
            col = table[name]
        elif name == "fetched_at" and fetched_at is not None:
            col = pa.array([fetched_at] * n, TS)
        elif name == "feed_last_updated" and feed_last_updated is not None:
            col = pa.array([feed_last_updated] * n, TS)
        elif name == "source":
            col = pa.array([source] * n, pa.string())
        else:
            col = pa.nulls(n, field.type)

        if name == "last_reported":
            col = epoch_to_ts(col)
        elif pa.types.is_boolean(field.type) and not pa.types.is_boolean(col.type):
            col = pc.not_equal(col.cast(pa.int64()), 0)
        elif pa.types.is_string(field.type) and pa.types.is_binary(col.type):
            col = col.cast(pa.string())
        cols[name] = col.cast(field.type)
    return pa.table(cols, schema=STATUS_SCHEMA)


def normalize_info(table: pa.Table, fetched_at: datetime) -> pa.Table:
    """Coerce a legacy (2019-08..2026-09) station_info file into INFO_SCHEMA."""
    n = table.num_rows
    cols = {}
    for field in INFO_SCHEMA:
        name = field.name
        if name == "fetched_at":
            col = pa.array([fetched_at] * n, TS)
        elif name == "feed_last_updated" and "last_updated" in table.column_names:
            col = epoch_to_ts(table["last_updated"])
        elif name in table.column_names:
            col = table[name]
        else:
            col = pa.nulls(n, field.type)
        cols[name] = col.cast(field.type)
    return pa.table(cols, schema=INFO_SCHEMA)


def sort_status(table: pa.Table) -> pa.Table:
    return table.sort_by(SORT_KEYS)


def drop_exact_duplicates(table: pa.Table) -> pa.Table:
    """Drop rows identical in every column (the feed occasionally repeated
    stations, e.g. 2023-08-05..2023-09-07). Only meaningful when fetched_at
    is exact; legacy_hourly rows legitimately repeat within an hour.
    Row order is not preserved."""
    return table.group_by(table.column_names, use_threads=True).aggregate([]).select(
        table.column_names
    ).cast(table.schema)


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

LEGACY_NAME_RE = re.compile(r"juvenai-1-(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-")


def fetched_at_from_legacy_name(key: str) -> datetime:
    m = LEGACY_NAME_RE.search(key)
    if not m:
        raise ValueError(f"Unrecognized legacy filename: {key}")
    return datetime(*map(int, m.groups()), tzinfo=UTC)


def month_days(month: str) -> list[date]:
    year, mon = map(int, month.split("-"))
    day = date(year, mon, 1)
    days = []
    while day.month == mon:
        days.append(day)
        day = date.fromordinal(day.toordinal() + 1)
    return days


def previous_month(today: date) -> str:
    first = today.replace(day=1)
    last = date.fromordinal(first.toordinal() - 1)
    return f"{last:%Y-%m}"
