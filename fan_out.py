from io import StringIO
import json
from urllib import request
from urllib.parse import quote_plus

import boto3
import pandas as pd
from pyathena import connect
from pyathena.async_cursor import AsyncCursor
from sqlalchemy import create_engine
from sqlalchemy import text as sqltext


ATHENA_BUCKET = "aws-athena-query-results-069757761977-us-east-1"
S3_STAGING_DIR = f"s3://{ATHENA_BUCKET}/fan_out/"
CACHE_BUCKET = "insulator-citi-bikecaster"
CACHE_KEY = "status_cache.csv"


class DBMgr:

    def __init__(self, region_name="us-east-1", schema_name="default"):
        self.region_name = region_name
        self.schema_name = schema_name
        self._engine = None

    @property
    def engine(self):
        if self._engine is None:
            self.make_engine()
            return self._engine
        else:
            return self._engine

    @staticmethod
    def _aws_credentials():
        return boto3.Session().get_credentials().get_frozen_credentials()

    def _conn_str(self):
        creds = self._aws_credentials()
        access_key = quote_plus(creds.access_key)
        secret_key = quote_plus(creds.secret_key)
        staging_dir = quote_plus(S3_STAGING_DIR)
        return (
            f"awsathena+rest://{access_key}:{secret_key}"
            f"@athena.{self.region_name}.amazonaws.com:443/{self.schema_name}"
            f"?s3_staging_dir={staging_dir}"
        )

    def make_engine(self):
        self._engine = create_engine(self._conn_str())

    def df_from_sql(self, sql, params=None):
        """
        Parameters
        ----------
        sql
            The SQL query to execute
        params
            Variables to be passed into the query (to avoid SQL injection).
            Example:
                >>> sql = "SELECT * FROM users WHERE id = :user_id"
                >>> params = {"user_id": 1}
                >>> df = db.df_from_sql(sql, params=params)
        Returns
        -------
        df
            The output of the query as a DataFrame
        """
        params = params or {}
        return pd.read_sql_query(sqltext(sql), self.engine, params=params)


db = DBMgr()


def pull_data():
    sql = """
    SELECT
      station_id,
      from_unixtime(last_reported) AS last_reported,
      num_bikes_available
    FROM citi_bikecaster.station_status
    WHERE
      date_parse(
        CAST(year AS VARCHAR) 
        || '-' || CAST(month AS VARCHAR) 
        || '-' || CAST(day AS VARCHAR)
        , '%Y-%m-%d') > current_timestamp - INTERVAL '8' day
    """
    cursor = connect(
        s3_staging_dir=S3_STAGING_DIR,
        region_name="us-east-1",
        cursor_class=AsyncCursor
    ).cursor()
    query_id, future = cursor.execute(sql)
    return future.result()


def load_data_cache():
    s3 = boto3.resource("s3")
    response = s3.Object(CACHE_BUCKET, CACHE_KEY).get()
    return pd.read_csv(
        StringIO(
            response["Body"]
            .read()
            .decode("utf-8")
        )
    )


def call_forecasters(station_ids):
    client = boto3.client("lambda")
    for station_id in station_ids:
        client.invoke(
            FunctionName="citi-bikecaster-model-prod-forecast",
            Payload=json.dumps({"station_id": station_id}),
            InvocationType="Event"
        )


def handler(event, context):
    result = pull_data()
    filename = result.output_location.split("/")[-1]
    # Update the cache
    s3 = boto3.resource("s3")
    copy_source = {
        "Bucket": ATHENA_BUCKET,
        "Key": f"fan_out/{filename}"
    }
    bucket = s3.Bucket(CACHE_BUCKET)
    bucket.copy(copy_source, CACHE_KEY)

    df = load_data_cache()
    url = "http://54.196.252.46"
    request.urlopen(request.Request("http://54.196.252.46", data="".encode("utf-8")), timeout=600)
    # call_forecasters(df["station_id"].unique().tolist())
