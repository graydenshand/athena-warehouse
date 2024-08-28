from datetime import datetime, timezone
from uuid import uuid4
import requests
import enum
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from openlineage.client import OpenLineageClient
from economic_data import config
from openlineage.client.facet import BaseFacet

class EventType(enum.Enum):
    """"""
    START = "START"
    COMPLETE = "COMPLETE"
    RUNNING = "RUNNING"
    ABORT = "ABORT"
    FAIL = "FAIL"
    OTHER = "OTHER"



def make_state_machine_lineage():
    client = OpenLineageClient.from_environment()
    producer = "economic_data.fetch_fred_data_state_machine"
    
    fred_api = Dataset(namespace="default", name="FRED API")
    fred_raw_tables = []
    for series_info in config.catalog.values():
        fred_raw_tables.append(Dataset(namespace="default", name=f"fred_raw.{series_info['name']}"))
    
    job = Job(namespace="default", name="fetch_fred_data_state_machine")
    run = Run(runId=str(uuid4()), facets={
        "trigger": {
            "rule": {
                "arn": "arn:aws:events:us-east-1:671532335163:rule/EconDataWarehouse-FredDat-FetchFredDataManualRule02-3eW8WWH6Pav0",
                "description": "Manually trigger fetching up-to-date data from FRED.",
                "event_pattern": {
                    "detail-type": ["TriggerFetchFredData"]
                },
                "type": "standard"
            },
        },
        "event_subscriptions": {
            "rules": [
                {
                    "arn": "arn:aws:events:us-east-1:671532335163:rule/EconDataWarehouse-FredDat-FetchFredDataManualRule02-3eW8WWH6Pav0",
                    "description": "Manually trigger fetching up-to-date data from FRED.",
                    "event_pattern": {
                        "detail-type": ["TriggerFetchFredData"]
                    },
                    "type": "standard"
                },
                {
                    "arn": "arn:aws:events:us-east-1:671532335163:rule/EconDataWarehouse-FredDat-FetchFredDataScheduledRul-PbUDpGubWEoB",
                    "description": "An event that fires on set intervals to trigger FRED data ingestion.",
                    "type": "scheduled standard",
                    "event_schedule": {
                        "type": "fixed_rate",
                        "rate": "7 days"
                    }
                },
            ]
        },
        "events_published": {
            "events": [
                {
                    "detail_type": config.updated_fred_raw_data_detail_type,
                    "source": "FredDataStateMachine",
                    "detail": {
                        "database": config.raw_db_name,
                    }
                }
            ]
        }
    })

    client.emit(
        RunEvent(
            RunState.START,
            datetime.now().isoformat(),
            run,
            job,
            producer,
            inputs=[fred_api],
            outputs=fred_raw_tables,
        )
    )

    client.emit(
        RunEvent(
            RunState.COMPLETE,
            datetime.now().isoformat(),
            run,
            job,
            producer,
            inputs=[fred_api],
            outputs=fred_raw_tables,
        )
    )

def make_join_tables_lineage():
    client = OpenLineageClient.from_environment()
    producer = "economic_data.join_tables_lambda"
    
    fred_raw_tables = []
    for series_info in config.catalog.values():
        fred_raw_tables.append(Dataset(namespace="default", name=f"fred_raw.{series_info['name']}"))
    economic_data_table = Dataset(namespace="default", name="warehouse.economic_data")
    
    job = Job(namespace="default", name="join_tables")
    run = Run(runId=str(uuid4()), facets={
        "trigger": {
            "rule": {   
                "arn": "arn:aws:events:us-east-1:671532335163:rule/EconDataWarehouse-RefreshEconomicDataTableRule293ED-WbldW8y1lGhf",
                "description": "When raw FRED data is updated, rebuild the economic_data table.",
                "event_pattern": {
                    "detail-type": [config.updated_fred_raw_data_detail_type],
                    "detail": {
                        "database": ["fred_raw"]
                    }
                },
                "type": "standard"
            },
        },
        "event_subscriptions": {
            "rules": [
                {   
                    "arn": "arn:aws:events:us-east-1:671532335163:rule/EconDataWarehouse-RefreshEconomicDataTableRule293ED-WbldW8y1lGhf",
                    "description": "When raw FRED data is updated, rebuild the economic_data table.",
                    "event_pattern": {
                        "detail-type": [config.updated_fred_raw_data_detail_type],
                        "detail": {
                            "database": ["fred_raw"]
                        }
                    },
                    "type": "standard"
                }
            ]
        },
        "events_published": {
            "events": [
                {
                    "detail_type": config.updated_economic_data_table_detail_type,
                    "source": "JoinFredTablesFunction",
                    "detail": {
                        "database": config.warehouse_db_name,
                    }
                }
            ]
        }
    })

    client.emit(
        RunEvent(
            RunState.START,
            datetime.now().isoformat(),
            run,
            job,
            producer,
            inputs=fred_raw_tables,
            outputs=[economic_data_table],
        )
    )

    client.emit(
        RunEvent(
            RunState.COMPLETE,
            datetime.now().isoformat(),
            run,
            job,
            producer,
            inputs=fred_raw_tables,
            outputs=[economic_data_table],
        )
    )


