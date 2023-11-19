import logging
import os

import boto3
import click

from economic_data import athena, config

logger = logging.getLogger(__name__)
events = boto3.client("events")


@click.group
def cli():
    """Utils for economic data warehouse."""
    logging.basicConfig(level=logging.WARNING)
    logger.setLevel(logging.DEBUG)
    config.initialize(s3_bucket_name=os.environ.get("S3_BUCKET_NAME"))


@cli.command()
def bootstrap_database():
    """Create glue/athena resources that this project expects to exist.

    This operation is safe to run multiple times. It will only create new
    resources that don't already exist.
    """
    logger.info("Create raw database")
    athena.create_database(config.raw_db_name)

    logger.info("Create data warehouse")
    athena.create_database(config.warehouse_db_name)

    for series_id in config.catalog:
        logger.info(f"Create table for series {series_id}")
        athena.create_raw_table(series_id)


@cli.command()
def trigger_fetch_data():
    """Emit an event bridge event to trigger the fetch FRED data state machine."""
    events.put_events(
        Entries=[
            dict(
                Source="EconomicDataCLI",
                DetailType=config.trigger_fetch_fred_raw_data_detail_type,
            )
        ]
    )


if __name__ == "__main__":
    cli()
