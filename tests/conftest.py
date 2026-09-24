import json
from pathlib import Path

import boto3
from moto import mock_aws
import pytest

from bikecaster import common

DATA = Path(__file__).parent / "data"


@pytest.fixture(autouse=True)
def aws(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        common._s3 = None
        client = boto3.client("s3")
        client.create_bucket(Bucket=common.BUCKET)
        yield client
    common._s3 = None


@pytest.fixture
def gbfs_status():
    return json.loads((DATA / "gbfs_status.json").read_text())


@pytest.fixture
def gbfs_info():
    return json.loads((DATA / "gbfs_info.json").read_text())


def put_file(client, path: Path, key: str):
    client.put_object(Bucket=common.BUCKET, Key=key, Body=path.read_bytes())


def keys(client, prefix: str) -> list[str]:
    return sorted(o["Key"] for o in common.list_objects(prefix))
