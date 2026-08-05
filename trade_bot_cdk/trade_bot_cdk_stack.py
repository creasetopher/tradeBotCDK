from aws_cdk import (
    BundlingOptions,
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    TimeZone,
    Tags,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_kinesis as kinesis,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_logs as logs,
    aws_s3 as s3,
    aws_scheduler as scheduler,
    aws_scheduler_targets as scheduler_targets,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    aws_sqs as sqs,
    aws_ssm as ssm,
)
from constructs import Construct


class TradeBotCdkStack(Stack):
    """Infrastructure for the trade bot research/data platform.

    This version separates:
      - candidate discovery: scheduled Lambda -> SNS -> SQS -> DynamoDB
      - market data ingestion: long-running ECS/Fargate worker -> Kinesis
      - historical storage: Kinesis -> Lambda -> S3 raw archive + DynamoDB hot/recent state
      - controls: SSM parameters for kill switch / trading enablement
    """

    def __init__(
            self, 
            scope: Construct, 
            construct_id: str, 
            stage: str = "dev",
            **kwargs
        ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        prefix = f"tradebot-{stage}"

        market_data_bucket = s3.Bucket(
            self,
            "MarketDataBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="MoveRawDataToInfrequentAccess",
                    prefix="raw/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30),
                        )
                    ],
                )
            ],
        )

        candidates_table = dynamodb.Table(
            self,
            "CandidatesTable",
            table_name=f"{prefix}-candidates",
            partition_key=dynamodb.Attribute(
                name="symbol",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="update_time",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            time_to_live_attribute="expires_at",
            removal_policy=RemovalPolicy.RETAIN,
        )

        active_candidates_table = dynamodb.Table(
            self,
            "ActiveCandidatesTable",
            table_name=f"{prefix}-active-candidates",
            partition_key=dynamodb.Attribute(
                name="universe_id",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="symbol",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            time_to_live_attribute="expires_at",
            removal_policy=RemovalPolicy.RETAIN,
        )


        market_events_table = dynamodb.Table(
            self,
            "MarketEventsTable",
            table_name=f"{prefix}-market-events-recent",
            partition_key=dynamodb.Attribute(
                name="symbol_day",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="event_ts_ms",
                type=dynamodb.AttributeType.NUMBER,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            time_to_live_attribute="expires_at",
            removal_policy=RemovalPolicy.RETAIN,
        )

        bot_state_table = dynamodb.Table(
            self,
            "BotStateTable",
            table_name=f"{prefix}-bot-state",
            partition_key=dynamodb.Attribute(
                name="pk",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="sk",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        market_event_stream = kinesis.Stream(
            self,
            "MarketEventStream",
            stream_name=f"{prefix}-market-events",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        # Candidate discovery fan-out: scanner Lambda publishes candidate snapshots to SNS.
        candidate_topic = sns.Topic(
            self,
            "CandidateUpdatesTopic",
            topic_name=f"{prefix}-candidate-updates",
            display_name=f"{prefix} candidate updates",
            enforce_ssl=True,
        )

        candidate_queue_dlq = sqs.Queue(
            self,
            "CandidateUpdatesDlq",
            queue_name=f"{prefix}-candidate-updates-dlq",
            enforce_ssl=True,
            retention_period=Duration.days(14),
        )

        candidate_queue_name = f"{prefix}-candidate-updates"

        candidate_queue = sqs.Queue(
            self,
            "CandidateUpdatesQueue",
            queue_name=candidate_queue_name,
            enforce_ssl=True,
            receive_message_wait_time=Duration.seconds(10),
            retention_period=Duration.days(4),
            visibility_timeout=Duration.seconds(120),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=5,
                queue=candidate_queue_dlq,
            ),
        )

        candidate_topic.add_subscription(
            sns_subscriptions.SqsSubscription(
                candidate_queue,
                raw_message_delivery=True,
            )
        )

        Tags.of(candidate_queue).add("name", candidate_queue_name)


        scanner_schedule_dlq = sqs.Queue(
            self,
            "ScannerScheduleDlq",
            queue_name=f"{prefix}-scanner-schedule-dlq",
            enforce_ssl=True,
            retention_period=Duration.days(14),
        )

        scanner_function = _lambda.Function(
            self,
            "ScannerFunction",
            function_name=f"{prefix}-scanner",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="scanner_handler.handler",
            code=self._bundled_python_lambda_code("lambda_functions/scanner"),
            memory_size=512,
            timeout=Duration.seconds(60),
            log_retention=logs.RetentionDays.ONE_MONTH,
            environment={
                "CANDIDATE_TOPIC_ARN": candidate_topic.topic_arn,
                "SCREENS": "small_cap_gainers,day_gainers",
                "MAX_SYMBOLS_PER_SCREEN": "100",
                "MIN_PRICE": "1.00",
                "MIN_DOLLAR_VOLUME": "1000000",
                "STAGE": stage,
            },
        )
        candidate_topic.grant_publish(scanner_function)

        candidate_writer_function = _lambda.Function(
            self,
            "CandidateWriterFunction",
            function_name=f"{prefix}-candidate-writer",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="candidate_ddb_writer.handler",
            code=self._bundled_python_lambda_code(
                "lambda_functions/market_candidate_ddb_writer"
            ),
            memory_size=256,
            timeout=Duration.seconds(60),
            log_retention=logs.RetentionDays.ONE_MONTH,
            environment={
                "CANDIDATES_TABLE_NAME": candidates_table.table_name,
                "UNIVERSE_ID": "default",
                "CANDIDATE_TTL_DAYS": "14",
                "ACTIVE_CANDIDATES_TABLE_NAME": active_candidates_table.table_name,
                "ACTIVE_CANDIDATE_TTL_SECONDS": "900",
                "STAGE": stage,
            },
        )
        candidates_table.grant_read_write_data(candidate_writer_function)
        candidate_writer_function.add_event_source(
            lambda_event_sources.SqsEventSource(
                candidate_queue,
                batch_size=10,
                max_batching_window=Duration.seconds(10),
                report_batch_item_failures=True,
            )
        )

        market_event_writer_dlq = sqs.Queue(
            self,
            "MarketEventWriterDlq",
            queue_name=f"{prefix}-market-event-writer-dlq",
            enforce_ssl=True,
            retention_period=Duration.days(14),
        )

        market_event_writer_function = _lambda.Function(
            self,
            "MarketEventWriterFunction",
            function_name=f"{prefix}-market-event-writer",
            runtime=_lambda.Runtime.PYTHON_3_12,
            architecture=_lambda.Architecture.ARM_64,
            handler="market_update_s3_writer.handler",
            code=_lambda.Code.from_asset("lambda_functions/market_update_s3_writer"),
            memory_size=512,
            timeout=Duration.seconds(120),
            log_retention=logs.RetentionDays.ONE_MONTH,
            environment={
                "MARKET_DATA_BUCKET_NAME": market_data_bucket.bucket_name,
                "MARKET_EVENTS_TABLE_NAME": market_events_table.table_name,
                "MARKET_EVENT_TTL_DAYS": "7",
                "STAGE": stage,
            },
        )
        market_data_bucket.grant_put(market_event_writer_function)
        market_events_table.grant_write_data(market_event_writer_function)
        market_event_stream.grant_read(market_event_writer_function)
        market_event_writer_function.add_event_source(
            lambda_event_sources.KinesisEventSource(
                market_event_stream,
                starting_position=_lambda.StartingPosition.LATEST,
                batch_size=100,
                max_batching_window=Duration.seconds(10),
                bisect_batch_on_error=True,
                report_batch_item_failures=True,
                retry_attempts=3,
                max_record_age=Duration.hours(2),
                on_failure=lambda_event_sources.SqsDlq(market_event_writer_dlq),
            )
        )

        # The scanner is time-boxed and runs in Lambda.
        # Market hours/holidays gating should still live in the function code because 
        # exchange holidays cannot be expressed in a cron job.
        scanner_target = scheduler_targets.LambdaInvoke(
            scanner_function,
            input=scheduler.ScheduleTargetInput.from_object(
                {
                    "source": "eventbridge-scheduler",
                    "job": "candidate-scan",
                    "stage": stage,
                }
            ),
            dead_letter_queue=scanner_schedule_dlq,
            max_event_age=Duration.minutes(5),
            retry_attempts=1,
        )
        scheduler.Schedule(
            self,
            "ScannerWeekdayMarketHoursSchedule",
            schedule_name=f"{prefix}-scanner-weekday-market-hours",
            description="Run candidate discovery during regular weekday market hours. Lambda code should still enforce exchange holiday checks.",
            schedule=scheduler.ScheduleExpression.cron(
                minute="*/5",
                hour="9-16",
                week_day="MON-FRI",
                time_zone=TimeZone.AMERICA_NEW_YORK,
            ),
            target=scanner_target,
        )

        # SAFETY SWITCHES: SSM parameters that must be manually toggled to enable live trading or disable all order execution.
        trading_enabled_param = ssm.StringParameter(
            self,
            "TradingEnabledParameter",
            parameter_name=f"/tradebot/{stage}/trading-enabled",
            string_value="false",
            description="Set to true only when live trading is intentionally enabled.",
        )

        # in case something really bad happesns, this kills all order execution immediately without needing to wait for parameter cache expiration
        kill_switch_param = ssm.StringParameter(
            self,
            "KillSwitchParameter",
            parameter_name=f"/tradebot/{stage}/kill-switch",
            string_value="true",
            description="If true, execution services must refuse all new orders.",
        )

        vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                )
            ],
        )
        cluster = ecs.Cluster(
            self,
            "Cluster",
            cluster_name=f"{prefix}-cluster",
            vpc=vpc,
            container_insights=True,
        )

        task_definition = ecs.FargateTaskDefinition(
            self,
            "MarketDataWorkerTaskDefinition",
            family=f"{prefix}-market-data-worker",
            cpu=256,
            memory_limit_mib=512,
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.ARM64,
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
        )

        task_definition.add_container(
            "MarketDataWorkerContainer",
            image=ecs.ContainerImage.from_asset("market_data_worker"),
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix=f"{prefix}-market-data-worker",
                log_retention=logs.RetentionDays.ONE_MONTH,
            ),
            environment={
                "MARKET_EVENT_STREAM_NAME": market_event_stream.stream_name,
                "ACTIVE_CANDIDATES_TABLE_NAME": active_candidates_table.table_name,
                "UNIVERSE_ID": "default",
                "TRADING_ENABLED_PARAM": trading_enabled_param.parameter_name,
                "KILL_SWITCH_PARAM": kill_switch_param.parameter_name,
                "STAGE": stage,
            },
        )

        market_event_stream.grant_write(task_definition.task_role)
        active_candidates_table.grant_read_write_data(candidate_writer_function)
        active_candidates_table.grant_read_data(task_definition.task_role)
        bot_state_table.grant_read_write_data(task_definition.task_role)
        trading_enabled_param.grant_read(task_definition.task_role)
        kill_switch_param.grant_read(task_definition.task_role)

        worker_security_group = ec2.SecurityGroup(
            self,
            "MarketDataWorkerSecurityGroup",
            vpc=vpc,
            description="Outbound-only security group for market data worker.",
        )

        market_data_worker_service = ecs.FargateService(
            self,
            "MarketDataWorkerService",
            service_name=f"{prefix}-market-data-worker",
            cluster=cluster,
            task_definition=task_definition,
            desired_count=0,
            assign_public_ip=True,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_groups=[worker_security_group],
            enable_execute_command=True,
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
        )

        CfnOutput(self, "MarketDataBucketName", value=market_data_bucket.bucket_name)
        CfnOutput(self, "MarketEventStreamName", value=market_event_stream.stream_name)
        CfnOutput(self, "CandidatesTableName", value=candidates_table.table_name)
        CfnOutput(self, "MarketEventsTableName", value=market_events_table.table_name)
        CfnOutput(self, "BotStateTableName", value=bot_state_table.table_name)
        CfnOutput(self, "CandidateTopicArn", value=candidate_topic.topic_arn)
        CfnOutput(self, "CandidateQueueUrl", value=candidate_queue.queue_url)
        CfnOutput(self, "ScannerFunctionName", value=scanner_function.function_name)
        CfnOutput(self, "MarketDataWorkerClusterName", value=cluster.cluster_name)
        CfnOutput(
            self,
            "MarketDataWorkerServiceName",
            value=market_data_worker_service.service_name,
        )
        CfnOutput(self, "TradingEnabledParamName", value=trading_enabled_param.parameter_name)
        CfnOutput(self, "KillSwitchParamName", value=kill_switch_param.parameter_name)
        CfnOutput(
            self,
            "ActiveCandidatesTableName",
            value=active_candidates_table.table_name,
        )

    @staticmethod
    def _bundled_python_lambda_code(path: str) -> _lambda.Code:
        return _lambda.Code.from_asset(
            path,
            bundling=BundlingOptions(
                image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                command=[
                    "bash",
                    "-c",
                    "if [ -f requirements.txt ]; then "
                    "python -m pip install -r requirements.txt -t /asset-output; "
                    "fi && cp -au . /asset-output",
                ],
            ),
        )
    
