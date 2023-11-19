import json
import logging
import os

import boto3

from economic_data import athena, config
from economic_data.fred import fetch_series, write_csv

logger = logging.getLogger(__name__)

secretsmanager = boto3.client("secretsmanager")
events = boto3.client("events")
fred_api_key_secret_arn = os.getenv("FRED_API_KEY_SECRET_ARN")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")


def get_api_key():
    if fred_api_key_secret_arn is None:
        raise EnvironmentError(
            "Environment variable not found: FRED_API_KEY_SECRET_ARN"
        )
    return secretsmanager.get_secret_value(SecretId=fred_api_key_secret_arn)[
        "SecretString"
    ]


def fetch_series_handler(event, context):
    """Fetch data for a series and write to S3 as gzipped csv file."""
    logger.info(f"Event: {event}")
    config.initialize(s3_bucket_name=s3_bucket_name)

    series_id = event["series_id"]
    series = config.catalog[series_id]
    logger.info(f"Series: {series}")

    data = fetch_series(series_id, api_key=get_api_key())
    logger.info(f"Data: [{data[0]}, ...]")

    path = f"{config.raw_data_path}/{series['name']}/{series['name']}.csv"
    logger.info(path)

    write_csv(path, ["day", "value"], data)
    logger.info("Complete")


def join_tables_handler(event, countext):
    """Join FRED raw data tables."""
    logger.info(f"Event: {event}")
    config.initialize(s3_bucket_name=s3_bucket_name)

    athena.build_joined_table()

    events.put_events(
        Entries=[
            dict(
                Source="JoinFredTablesFunction",
                DetailType=config.updated_economic_data_table_detail_type,
                Detail=json.dumps(dict(database=config.warehouse_db_name)),
            )
        ]
    )
