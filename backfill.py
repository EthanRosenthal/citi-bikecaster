from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time
import uuid

import numpy as np
import pandas as pd

EXECUTOR = ThreadPoolExecutor()


def ts_to_unixtime(series):
    return pd.to_datetime(series).astype(np.int64) // 10 ** 9


def s3_path(now: pd.Timestamp) -> str:
    bucket = "insulator-citi-bikecaster"
    key = (
        f"station_status/{now.year:04d}/{now.month:02d}/"
        f"{now.day:02d}/{now.hour:02d}/"
        f"juvenai-1-{now.year:04d}-{now.month:02}-{now.day:02d}-"
        f"{now.hour:02d}-{now.minute:02d}-{now.second:02d}-"
        f"{str(uuid.uuid4())}.parquet"
    )
    return f"s3://{bucket}/{key}"


def write_frame(frame, execution_time):
    frame.to_parquet(
        s3_path(execution_time),
        engine="fastparquet",
        compression="snappy"
    )
    return True


def run(path, max_timestamp):
    renames = {
        "id": "station_id",
        "available_bikes": "num_bikes_available",
        "available_docks": "num_docks_available",
        "last_communication_time": "last_reported"
    }

    type_casts = {
        "station_id": str,
        "num_bikes_available": np.int64,
        "num_docks_available": np.int64,
    }

    keep_cols = [
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "last_reported"
    ]
    t0 = time.time()
    for ctr, chunk in enumerate(pd.read_csv(path, chunksize=1_000_000, sep="|")):

        print(f"Writing chunk {ctr}")

        chunk = chunk.rename(columns=renames)
        chunk = chunk.astype(type_casts)
        chunk["last_reported"] = ts_to_unixtime(chunk["last_reported"])
        chunk = chunk[["execution_time"] + keep_cols]
        chunk["execution_time"] = pd.to_datetime(chunk["execution_time"])
        chunk["partition_key"] = chunk["execution_time"].dt.strftime("%Y-%m-%d %H")
        futures = []
        for partition_key, group in chunk.groupby("partition_key"):

            group = group[group["execution_time"] < max_timestamp]
            if group.empty:
                continue

            futures.append(
                EXECUTOR.submit(
                    write_frame, group[keep_cols], pd.to_datetime(partition_key)
                )
            )

        for future in concurrent.futures.as_completed(futures):
            if not future.result():
                print("Some unknown error on completing a future")
        t1 = time.time()
        print(f"Done writing chunk {ctr}. Elapsed time: {t1 - t0:5.3f}s")
        t0 = t1


if __name__ == "__main__":
    import sys
    max_timestamp = pd.to_datetime("2019-08-01 18:52:03")
    print("Begin backfilling data")
    run(sys.argv[1], max_timestamp)
