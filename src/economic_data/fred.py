import csv
import os
import re
import tempfile
import tomllib
from datetime import date
from io import StringIO
from pathlib import Path

import boto3
import requests


def fetch_series(series_id: str, api_key: str) -> list[tuple[date, float]]:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = dict(series_id=series_id, api_key=api_key, file_type="json")
    data = requests.get(url, params=params).json()
    return [
        (
            date.fromisoformat(row["date"]),
            float(row["value"]) if row["value"] != "." else None,
        )
        for row in data["observations"]
    ]


def write_csv(
    file: os.PathLike, column_names: tuple[str, str], data: list[tuple[date, float]]
):
    """Write output from fetch_series to a csv file."""
    if str(file).startswith("s3://"):
        with StringIO() as f:
            writer = csv.writer(f)
            writer.writerows([column_names] + data)
            s3 = boto3.client("s3")
            pattern = r"s3://(.*?)/(.*)"
            match = re.match(pattern, file)
            bucket_name, key = match.groups()
            s3.put_object(
                Body=bytes(f.getvalue(), "utf-8"), Bucket=bucket_name, Key=key
            )
    else:
        with open(file, "w") as f:
            writer = csv.writer(f)
            writer.writerows([column_names] + data)
