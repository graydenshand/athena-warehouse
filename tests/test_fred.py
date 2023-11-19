"""Test FRED code. 

Tests will use a mocked API response if FRED_API_KEY environment variable is
not present.
"""

import json
import os
from datetime import date
from unittest.mock import MagicMock, patch
from uuid import uuid4

import boto3
from pytest import skip

from economic_data import config
from economic_data.fred import fetch_series, write_csv

fred_api_key = os.getenv("FRED_API_KEY")


sample_response = """\
{"realtime_start":"2023-11-11","realtime_end":"2023-11-11","observation_start":"1600-01-01","observation_end":"9999-12-31","units":"lin","output_type":1,"file_type":"json","order_by":"observation_date","sort_order":"asc","count":307,"offset":0,"limit":100000,"observations":[{"realtime_start":"2023-11-11","realtime_end":"2023-11-11","date":"1947-01-01","value":"2182.681"},{"realtime_start":"2023-11-11","realtime_end":"2023-11-11","date":"1947-04-01","value":"2176.892"},{"realtime_start":"2023-11-11","realtime_end":"2023-11-11","date":"1947-07-01","value":"2172.432"},{"realtime_start":"2023-11-11","realtime_end":"2023-11-11","date":"1947-10-01","value":"2206.452"},{"realtime_start":"2023-11-11","realtime_end":"2023-11-11","date":"1948-01-01","value":"2239.682"}]}
"""


def test_fetch_series():
    if fred_api_key:
        data = fetch_series("SP500", fred_api_key)
    else:
        mock_response = MagicMock()
        mock_response.json.return_value = json.loads(sample_response)
        with patch("economic_data.fred.requests.get", return_value=mock_response):
            data = fetch_series("GDPC1", "MOCK_API_KEY")
    assert len(data) > 0
    assert isinstance(data[0][0], date)
    assert isinstance(data[0][1], float)


def test_write_csv(tmp_path):
    write_csv(tmp_path / "data.csv", ("date", "value"), [("2023-11-12", 0.0)])
    assert (tmp_path / "data.csv").read_text() == "date,value\n2023-11-12,0.0\n"


def test_s3_write_csv():
    s3 = boto3.client("s3")
    key = f"pytest-{uuid4().hex}/data.csv"
    if config.s3_bucket_name:
        write_csv(
            f"s3://{config.s3_bucket_name}/{key}",
            ("date", "value"),
            [("2023-11-12", 0.0)],
        )
        s3.delete_object(Bucket=config.s3_bucket_name, Key=key)
    else:
        skip("S3 base path not configured")
