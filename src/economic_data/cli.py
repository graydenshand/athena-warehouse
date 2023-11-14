import click
from economic_data import config, athena
import os
import logging

logger = logging.getLogger(__name__)


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

    catalog = athena.load_catalog()
    for series_id in catalog:
        logger.info(f"Create table for series {series_id}")
        athena.create_raw_table(series_id)


if __name__ == "__main__":
    cli()
