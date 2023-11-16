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


def build_joined_table():
    drop_table = f"DROP TABLE IF EXISTS {config.warehouse_db_name}.economic_data;"

    select = ["d.day"] + [
        f"{config.raw_db_name}.{series['name']}.value as {series['name']}"
        for series in config.catalog.values()
    ]
    join_stmts = [
        f"LEFT JOIN {config.raw_db_name}.{series['name']} ON d.day = {config.raw_db_name}.{series['name']}.day"
        for series in config.catalog.values()
    ]
    conditions = [
        f"{config.raw_db_name}.{series['name']}.value IS NOT NULL"
        for series in config.catalog.values()
    ]
    create_table = f"""
    CREATE TABLE {config.warehouse_db_name}.economic_data 
    WITH ( table_type = 'ICEBERG', location = '{config.warehouse_path}/economic_data', is_external = false )
    AS (
        SELECT {','.join(select)}
        FROM {config.warehouse_db_name}.days d
        {"\n".join(join_stmts)}
        WHERE {"\nOR ".join(conditions)}
        ORDER BY DAY
    );"""
    execute_sql([drop_table, create_table])


def create_database(db_name: str):
    execute_sql(f"CREATE DATABASE IF NOT EXISTS {db_name};")


def create_raw_table(series_id: str):
    """Create a table in the fred raw database to store data for a single series."""
    series = config.catalog[series_id]
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
    LOCATION '{config.raw_data_path}/{name}/'
    TBLPROPERTIES("skip.header.line.count"="1");"""
    execute_sql(sql)


def create_days_table():
    """Create a table with dates from 1900-2100 for simpler join logic."""
    create_table_sql = f"""\
    CREATE TABLE IF NOT EXISTS {config.warehouse_db_name}.days (
        day DATE
    )
    COMMENT 'Dates for simpler joins'
    LOCATION '{config.warehouse_path}/days/'
    TBLPROPERTIES ( 'table_type' = 'ICEBERG');
    """
    insert_sql = f"""
    INSERT INTO {config.warehouse_db_name}.days (
        SELECT day 
        FROM (
        SELECT
            sequence(date %(start)s, date %(end)s, interval '1' day) days
        ) d
        CROSS JOIN UNNEST(days) as t(day)
    )
    """
    execute_sql([create_table_sql, insert_sql, insert_sql], [None, dict(start="1900-01-01", end="1999-12-31"), dict(start="2000-01-01", end="2099-12-31")])