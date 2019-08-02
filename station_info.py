from datetime import datetime as dt
import json
import logging
import uuid

from pandas.io.json import json_normalize
import requests

logger = logging.getLogger()

INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
SAVE_KEYS = [
    "station_id",
    "name",
    "lat",
    "lon",
    "region_id",
    "capacity",
    "has_kiosk",
]


def s3_path():
    now = dt.utcnow()
    bucket = "insulator-citi-bikecaster"
    key = (
        f"station_info/{now.year:04d}/{now.month:02d}/"
        f"{now.day:02d}/{now.hour:02d}/"
        f"juvenai-1-{now.year:04d}-{now.month:02}-{now.day:02d}-"
        f"{now.hour:02d}-{now.minute:02d}-{now.second:02d}-"
        f"{str(uuid.uuid4())}.parquet"
    )
    return f"s3://{bucket}/{key}"


def handler(event, context):
    try:
        res = requests.get(INFO_URL).json()
    except Exception as exc:
        logger.exception("Failed to get and parse station info")
        raise exc

    station_data = json_normalize(res["data"]["stations"])
    station_data = station_data[SAVE_KEYS]
    station_data["last_updated"] = res["last_updated"]
    station_data.to_parquet(s3_path(), engine="fastparquet", compression="snappy")

    body = {
        "message": "Successfully logged station info",
        "input": event,
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response


if __name__ == "__main__":
    handler("Test Event", None)
