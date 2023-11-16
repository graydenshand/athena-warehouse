import aws_cdk as cdk
from constructs import Construct
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from aws_cdk import aws_lambda
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_secretsmanager as secretsmanager
from economic_data import config
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as events_targets



class FredDataStateMachine(cdk.NestedStack):

    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)
        secret = secretsmanager.Secret(
            self, "FredAPISecret", description="API key for FRED api."
        )
        fetch_fred_data_fn = aws_lambda.DockerImageFunction(
            self,
            "FetchFredDataFunction",
            code=aws_lambda.DockerImageCode.from_ecr(
                repository=scope.image.repository,
                tag_or_digest=scope.image.image_tag,
                cmd=["economic_data.lambda_handlers.fetch_series_handler"],
            ),
            environment={
                "FRED_API_KEY_SECRET_ARN": secret.secret_full_arn,
                "S3_BUCKET_NAME": scope.bucket.bucket_name,
                "LOG_LEVEL": "INFO",
            },
            timeout=cdk.Duration.seconds(60),
        )
        secret.grant_read(fetch_fred_data_fn.role)
        scope.bucket.grant_read_write(fetch_fred_data_fn)

        sfn_fetch_data_map = sfn.Map(
            self,
            "ParallelFetchDataTask",
            items_path=sfn.JsonPath.string_at("$.series"),
        )

        fetch_fred_data_task = sfn_tasks.LambdaInvoke(
            self, "FetchFredDataTask", lambda_function=fetch_fred_data_fn
        )

        sfn_fetch_data_map.iterator(fetch_fred_data_task)

        self.send_update_raw_data_event = sfn_tasks.EventBridgePutEvents(
            self,
            "SendUpdatedFredRawDataEvent",
            entries=[
                sfn_tasks.EventBridgePutEventsEntry(
                    detail=sfn.TaskInput.from_object(
                        {
                            "database": config.raw_db_name,
                        }
                    ),
                    detail_type="UpdatedFredRawData",
                    source="FredDataStateMachine",
                )
            ],
        )

        state_machine_definition = sfn_fetch_data_map.next(self.send_update_raw_data_event)

        state_machine = sfn.StateMachine(
            self,
            "FredDataStateMachine",
            comment="Fetch latest FRED data for specified series.",
            definition_body=sfn.DefinitionBody.from_chainable(state_machine_definition),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        rule = events.Rule(
            self,
            "WeeklyFredTrigger",
            schedule=events.Schedule.rate(cdk.Duration.days(7)),
            description="An event that fires weekly to trigger FRED data ingestion."
        )

        rule.add_target(
            events_targets.SfnStateMachine(
                state_machine,
                input=events.RuleTargetInput.from_object(
                    {"series": [
                        {"series_id": s} 
                        for s in config.catalog
                    ]}
                ),
            )
        )

        cdk.CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        cdk.CfnOutput(
            self, "FetchDataFunctionName", value=fetch_fred_data_fn.function_name
        )
        cdk.CfnOutput(self, "FredApiKeySecretArn", value=secret.secret_full_arn)
        


class EconomicDataWarehouseStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        self.bucket = s3.Bucket(self, "Bucket", removal_policy=cdk.RemovalPolicy.DESTROY, versioned=True)
        self.bucket.add_lifecycle_rule(
            expiration=cdk.Duration.days(1), prefix="athena_results/"
        )

        self.image = ecr_assets.DockerImageAsset(
            self, "DockerImage", platform=ecr_assets.Platform.LINUX_AMD64, directory="."
        )

        self.fred_data_state_machine = FredDataStateMachine(self, "FredDataStateMachine")

        self.join_tables_fn = aws_lambda.DockerImageFunction(
            self,
            "JoinFredTablesFunction",
            code=aws_lambda.DockerImageCode.from_ecr(
                repository=scope.image.repository,
                tag_or_digest=scope.image.image_tag,
                cmd=["economic_data.lambda_handlers.join_tables_handler"],
            ),
            environment={
                "S3_BUCKET_NAME": scope.bucket.bucket_name,
                "LOG_LEVEL": "INFO",
            },
            timeout=cdk.Duration.seconds(60),
        )


        cdk.CfnOutput(self, "BucketName", value=self.bucket.bucket_name)
        cdk.CfnOutput(self, "ImageUri", value=self.image.image_uri)



if __name__ == "__main__":
    app = cdk.App()
    EconomicDataWarehouseStack(app, "EconDataWarehouse")
    app.synth()
