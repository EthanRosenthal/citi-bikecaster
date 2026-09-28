"""Run an Athena query and print the results as TSV.

    uv run scripts/athena.py [--workgroup citibike] "SELECT ..."
"""

import argparse
import sys
import time

import boto3


def run(sql: str, workgroup: str, database: str | None = None) -> list[list[str]]:
    athena = boto3.client("athena")
    kwargs = {"QueryString": sql, "WorkGroup": workgroup}
    if database:
        kwargs["QueryExecutionContext"] = {"Database": database}
    qid = athena.start_query_execution(**kwargs)["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"{status['State']}: {status.get('StateChangeReason')}")
    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue", "") for c in row["Data"]])
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("sql")
    parser.add_argument("--workgroup", default="citibike")
    parser.add_argument("--database")
    args = parser.parse_args()
    for row in run(args.sql, args.workgroup, args.database):
        sys.stdout.write("\t".join(row) + "\n")
