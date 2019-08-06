from datetime import datetime as dt
from datetime import timedelta
import json
import logging
import re
import time
import uuid

import boto3
from pandas.io.json import json_normalize
import requests

logger = logging.getLogger()

STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
SAVE_KEYS = [
    "station_id",
    "num_bikes_available",
    "num_ebikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
]
BUCKET = "insulator-citi-bikecaster"


def s3_partition(now):
    return (
        f"station_status/{now.year:04d}/{now.month:02d}/"
        f"{now.day:02d}/{now.hour:02d}"
    )


def s3_filename(now, file_id):
    return (
        f"juvenai-1-{now.year:04d}-{now.month:02}-{now.day:02d}-"
        f"{now.hour:02d}-{now.minute:02d}-{now.second:02d}-"
        f"{file_id}.parquet"
    )


def s3_path(now, file_id):
    return f"s3://{BUCKET}/{s3_partition(now)}/{s3_filename(now, file_id)}"


def handler(event, context):
    try:
        res = requests.get(STATUS_URL).json()
    except Exception as exc:
        logger.exception("Failed to get and parse station data")
        raise exc

    station_data = json_normalize(res["data"]["stations"])
    station_data = station_data[SAVE_KEYS]
    now = dt.now()
    file_id = str(uuid.uuid4())
    station_data.to_parquet(
        s3_path(now, file_id), engine="fastparquet", compression="snappy"
    )

    body = {"message": "Successfully logged station statuses", "input": event}

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response


def poll_query_success(client, execution_id, max_polls=120):
    state = "RUNNING"
    execution_number = 0
    while execution_number < max_polls and state in ("RUNNING"):
        query_res = client.get_query_execution(QueryExecutionId=execution_id)
        if (
            "QueryExecution" in query_res
            and "Status" in query_res["QueryExecution"]
            and "State" in query_res["QueryExecution"]["Status"]
        ):
            state = query_res["QueryExecution"]["Status"]["State"]
            if state == "FAILED":
                return False
            elif state == "SUCCEEDED":
                path = query_res["QueryExecution"]["ResultConfiguration"][
                    "OutputLocation"
                ]
                filename = re.findall(".*\/(.*)", path)[0]
                return filename
        time.sleep(1)
        execution_number += 1
    return False


def add_partitions(event, context):
    today = dt.combine(dt.now().date(), dt.min.time())
    tomorrow = today + timedelta(days=1)
    hour_partitions = [tomorrow + timedelta(hours=hour) for hour in range(0, 24)]

    partition_query = "\n".join(
        [
            (
                f"PARTITION (year = '{p.year:04d}', month = '{p.month:02d}', day = '{p.day:02d}', hour = '{p.hour:02d}') "
                f"LOCATION 's3://{BUCKET}/{s3_partition(p)}/'"
            )
            for p in hour_partitions
        ]
    )

    query = f"""
    ALTER TABLE station_status ADD IF NOT EXISTS
      {partition_query};
    """
    logger.info(query)

    client = boto3.client("athena")

    config = {
        "OutputLocation": "s3://aws-athena-query-results-069757761977-us-east-1/citi_bikecaster/repartition_station_status/",
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    }

    # Query Execution Parameters
    context = {"Database": "citi_bikecaster"}

    response = client.start_query_execution(
        QueryString=query, QueryExecutionContext=context, ResultConfiguration=config
    )
    logger.info("Polling query")
    success = poll_query_success(client, response["QueryExecutionId"])
    logger.info("Done polling query")

    if success:
        msg = "Succefully updated station_status partitions"
        status_code = 200
    else:
        msg = "Failed to updte partitions for some reason"
        status_code = 500
        logger.error(msg)

    body = {"message": msg, "input": event}

    response = {"statusCode": status_code, "body": json.dumps(body)}

    return response


if __name__ == "__main__":
    handler("Test Event", None)
