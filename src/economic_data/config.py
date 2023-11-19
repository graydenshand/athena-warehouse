"""Global values."""
import tomllib
from importlib.resources import files

import pydantic

s3_base_path: str
raw_data_path: str
warehouse_path: str
athena_results_path: str
# catalog: str = ""
with files("economic_data").joinpath("catalog.toml").open("rb") as f:
    catalog = tomllib.load(f)
raw_db_name = "fred_raw"
warehouse_db_name = "warehouse"
updated_economic_data_table_detail_type = "UpdatedEconomicDataTable"
updated_fred_raw_data_detail_type = "UpdatedFredRawData"
trigger_fetch_fred_raw_data_detail_type = "TriggerFetchFredData"

_initialized = False


class Config(pydantic.BaseModel):
    s3_bucket_name: str
    raw_db_name: str = pydantic.Field(default="fred_raw")
    warehouse_db_name: str = pydantic.Field(default="warehouse")

    @pydantic.computed_field
    @property
    def raw_data_path(self) -> str:
        return f"s3://{self.s3_bucket_name}/fred_raw_data"

    @pydantic.computed_field
    @property
    def warehouse_path(self) -> str:
        return f"s3://{self.s3_bucket_name}/warehouse"

    @pydantic.computed_field
    @property
    def athena_results_path(self) -> str:
        return f"s3://{self.s3_bucket_name}/athena_results"


def initialize(config: Config | None = None, **kwargs):
    if _initialized is True:
        raise RuntimeError("Config has already been initialized.")
    if config is None:
        config = Config(**kwargs)
    for k, v in config.model_dump().items():
        globals()[k] = v
