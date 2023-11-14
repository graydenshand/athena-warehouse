from importlib.resources import files
import tomllib
from economic_data import config
import pyathena
from typing import Any

RAW_DB_NAME = "fred_raw"
WAREHOUSE_NAME = "warehouse"


def execute_sql(
    sql: str | list[str], params: list[Any] | list[list[Any]] | None = None
):
    """Execute one or more sql queries."""
    if isinstance(sql, str):
        sql = [sql]
        params = [params]
    else:
        if params is None:
            params = [None] * len(sql)
    with pyathena.connect(s3_staging_dir=config.athena_results_path) as conn:
        with conn.cursor() as cursor:
            for i in range(len(sql)):
                cursor.execute(sql[i], params[i])
            result = cursor.fetchall()
    return result


def build_joined_table(event, context):
    drop_table = "DROP TABLE economic_data;"
    create_table = f"""\
    CREATE TABLE economic_data 
    WITH ( table_type = 'ICEBERG', location = '{config.warehouse_path}/', is_external = false )
    AS (
        SELECT coalesce(ur.day, m30.day) day, ur.value unemployment_rate, gdp.value real_gdp, m30.value mortgage_30yr
        FROM raw.unemployment_rate ur
        FULL OUTER JOIN raw.mortgage_30yr m30 ON m30.day = ur.day
        FULL OUTER JOIN raw.real_gdp gdp ON ur.day = gdp.day
    );"""
    execute_sql([drop_table, create_table])


def load_catalog() -> dict[str, dict[str, str]]:
    with files("economic_data").joinpath("catalog.toml").open("rb") as f:
        return tomllib.load(f)


def create_database(db_name: str):
    execute_sql(f"CREATE DATABASE IF NOT EXISTS {db_name};")


def create_raw_table(series_id: str):
    """Create a table in the fred raw database to store data for a single series."""
    catalog = load_catalog()
    series = catalog[series_id]
    name = series["name"]
    comment = series["comment"]
    sql = rf"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {RAW_DB_NAME}.{name} (
        day date,
        value float
    )
    COMMENT '{comment}'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    ESCAPED BY '\\'
    LINES TERMINATED BY '\n'
    LOCATION '{config.raw_data_path}/{series_id}/'
    TBLPROPERTIES("skip.header.line.count"="1");"""
    execute_sql(sql)
