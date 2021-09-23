# ######################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                  #
#                                                                                                                      #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance      #
#  with the License. You may obtain a copy of the License at                                                           #
#                                                                                                                      #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                      #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed    #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for   #
#  the specific language governing permissions and limitations under the License.                                      #
# ######################################################################################################################

from aws_cdk import core as cdk
from aws_cdk.aws_s3 import EventType, NotificationKeyFilter
from aws_cdk.aws_s3_notifications import LambdaDestination
from aws_cdk.aws_stepfunctions import (
    StateMachine,
    Chain,
    Parallel,
    TaskInput,
)
from aws_cdk.core import CfnCondition, Fn, Aws, Duration

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.resource_name import ResourceName
from aws_solutions.cdk.cfn_nag import (
    CfnNagSuppression,
    add_cfn_nag_suppressions,
    CfnNagSuppressAll,
)
from aws_solutions.cdk.stack import SolutionStack
from personalize.aws_lambda.functions import (
    S3EventHandler,
    CreateDatasetGroup,
    CreateSchema,
    CreateDataset,
    CreateDatasetImportJob,
    CreateEventTracker,
    CreateSolution,
    CreateSolutionVersion,
    CreateCampaign,
    CreateFilter,
    CreateBatchInferenceJob,
    CreateTimestamp,
)
from personalize.aws_lambda.layers import PowertoolsLayer, SolutionsLayer
from personalize.cloudwatch.dashboard import Dashboard
from personalize.s3 import AccessLogsBucket, DataBucket
from personalize.scheduler import Scheduler
from personalize.sns.notifications import Notifications
from personalize.step_functions.dataset_imports_fragment import DatasetImportsFragment
from personalize.step_functions.event_tracker_fragment import EventTrackerFragment
from personalize.step_functions.failure_fragment import FailureFragment
from personalize.step_functions.filter_fragment import FilterFragment
from personalize.step_functions.scheduled_dataset_import import ScheduledDatasetImport
from personalize.step_functions.scheduled_solution_maintenance import (
    ScheduledSolutionMaintenance,
)
from personalize.step_functions.scheduler_fragment import SchedulerFragment
from personalize.step_functions.schedules import Schedules
from personalize.step_functions.solution_fragment import SolutionFragment


class PersonalizeStack(SolutionStack):
    def __init__(
        self, scope: cdk.Construct, construct_id: str, *args, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, *args, **kwargs)

        # CloudFormation Parameters
        self.personalize_kms_key_arn = cdk.CfnParameter(
            self,
            id="PersonalizeKmsKeyArn",
            description="Provide Amazon Personalize with an alternate AWS Key Management (KMS) key to use to encrypt your datasets",
            default="",
            allowed_pattern="(^arn:.*:kms:.*:.*:key/.*$|^$)",
        )
        self.solutions_template_options.add_parameter(
            self.personalize_kms_key_arn,
            "(Optional) KMS key ARN used to encrypt Datasets managed by Amazon Personalize",
            "Security Configuration",
        )
        kms_enabled = cdk.CfnCondition(
            self,
            "PersonalizeSseKmsEnabled",
            expression=Fn.condition_not(
                Fn.condition_equals(self.personalize_kms_key_arn, "")
            ),
        )

        self.email = cdk.CfnParameter(
            self,
            id="Email",
            type="String",
            description="Email to notify with personalize workflow results",
            default="",
            max_length=50,
            allowed_pattern=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$|^$)",
            constraint_description="Must be a valid email address or blank",
        )
        self.solutions_template_options.add_parameter(
            self.email, "Email", "Solution Configuration"
        )
        self.email_provided = CfnCondition(
            self,
            "EmailProvided",
            expression=Fn.condition_not(Fn.condition_equals(self.email, "")),
        )

        # layers
        layer_powertools = PowertoolsLayer.get_or_create(self)
        layer_solutions = SolutionsLayer.get_or_create(self)
        common_layers = [layer_powertools, layer_solutions]

        # buckets
        access_logs_bucket = AccessLogsBucket(
            self,
            "AccessLogsBucket",
            suppress=[
                CfnNagSuppression(
                    "W35",
                    "This bucket is used as the logging destination for personalization data processing",
                )
            ],
        )

        data_bucket = DataBucket(
            self,
            "PersonalizeBucket",
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix="personalize-bucket-access-logs/",
        )

        # the AWS lambda functions required by the shared step functions
        create_dataset_group = CreateDatasetGroup(
            self,
            "Create Dataset Group",
            input_path="$.datasetGroup",  # NOSONAR (python:S1192) - string for clarity
            result_path="$.datasetGroup.serviceConfig",  # NOSONAR (python:S1192) - string for clarity
            kms_enabled=kms_enabled,
            kms_key=self.personalize_kms_key_arn,
            layers=common_layers,
        )
        create_schema = CreateSchema(
            self,
            "Create Schema",
            layers=common_layers,
        )
        create_dataset = CreateDataset(
            self,
            "Create Dataset",
            layers=common_layers,
        )
        create_dataset_import_job = CreateDatasetImportJob(
            self,
            "Create Dataset Import Job",
            layers=common_layers,
            personalize_bucket=data_bucket,
        )
        notifications = Notifications(
            self,
            "SNS Notification",
            email=self.email,
            email_provided=self.email_provided,
            layers=common_layers,
        )
        create_event_tracker = CreateEventTracker(
            self,
            "Create Event Tracker",
            layers=common_layers,
        )
        create_solution = CreateSolution(
            self,
            "Create Solution",
            layers=common_layers,
        )
        create_solution_version = CreateSolutionVersion(
            self,
            "Create Solution Version",
            layers=common_layers,
        )
        create_campaign = CreateCampaign(
            self,
            "Create Campaign",
            layers=common_layers,
        )
        create_batch_inference_job = CreateBatchInferenceJob(
            self,
            "Create Batch Inference Job",
            layers=common_layers,
            personalize_bucket=data_bucket,
        )
        create_filter = CreateFilter(self, "Create Filter", layers=common_layers)
        create_timestamp = CreateTimestamp(
            self, "Create Timestamp", layers=[layer_powertools]
        )

        dataset_management_functions = {
            "create_schema": create_schema,
            "create_dataset": create_dataset,
            "create_dataset_import_job": create_dataset_import_job,
        }

        success = notifications.state(
            self,
            "Success",
            payload=TaskInput.from_object(
                {"datasetGroup.$": "$[0].datasetGroup.serviceConfig.name"}
            ),
        )

        dataset_import_schedule_sfn = ScheduledDatasetImport(
            self,
            "Scheduled Dataset Import",
            dataset_management_functions=dataset_management_functions,
            create_timestamp=create_timestamp,
            notifications=notifications,
        ).state_machine

        solution_maintenance_schedule_sfn = ScheduledSolutionMaintenance(
            self,
            "Scheduled Solution Maintenance",
            create_solution=create_solution,
            create_solution_version=create_solution_version,
            create_campaign=create_campaign,
            create_batch_inference_job=create_batch_inference_job,
            create_timestamp=create_timestamp,
            notifications=notifications,
        ).state_machine

        # scheduler and step function to schedule
        scheduler = Scheduler(self, "Scheduler")
        scheduler.grant_invoke(dataset_import_schedule_sfn)
        scheduler.grant_invoke(solution_maintenance_schedule_sfn)

        schedules = Schedules(
            dataset_import=SchedulerFragment(
                self,
                schedule_for="personalize dataset import",
                schedule_for_suffix="$.datasetGroup.serviceConfig.name",
                scheduler=scheduler,
                target=dataset_import_schedule_sfn,
                schedule_path="$.datasetGroup.workflowConfig.schedules.import",
                schedule_input={
                    "datasetGroup": {
                        "serviceConfig.$": "$.datasetGroup.serviceConfig",
                        "workflowConfig": {"maxAge": "1 second"},
                    },  # NOSONAR (python:S1192) - string for clarity
                    "datasets.$": "$.datasets",
                    "bucket.$": "$.bucket",
                },
            ),
        )

        create_solutions = SolutionFragment(
            self,
            "Create Solutions",
            create_solution=create_solution,
            create_solution_version=create_solution_version,
            create_campaign=create_campaign,
            create_batch_inference_job=create_batch_inference_job,
            scheduler=scheduler,
            to_schedule=solution_maintenance_schedule_sfn,
        )

        # fmt: off
        definition = Chain.start(
            Parallel(self, "Manage The Execution")
            .branch(
                create_dataset_group.state(
                    self,
                    "Create Dataset Group",
                    backoff_rate=1.02,
                    interval=Duration.seconds(5),
                    max_attempts=30,
                )
                .next(
                    DatasetImportsFragment(
                        self,
                        "Handle Dataset Imports",
                        **dataset_management_functions
                    )
                ).next(
                    schedules.dataset_import
                ).next(
                    EventTrackerFragment(self, "Event Tracker", create_event_tracker=create_event_tracker)
                ).next(
                    FilterFragment(self, "Filters", create_filter=create_filter)  # filters require data to be present
                ).next(
                    create_solutions
                )
            )
            .add_catch(
                FailureFragment(self, notifications).start_state,
                errors=["States.ALL"],
                result_path="$.statesError"
            )
            .next(success)
        )
        # fmt: on

        state_machine_namer = ResourceName(
            self, "StateMachineName", purpose="personalize-workflow", max_length=80
        )
        state_machine = StateMachine(
            self,
            "PersonalizeStateMachine",
            state_machine_name=state_machine_namer.resource_name.to_string(),
            definition=definition,
            tracing_enabled=True,
        )
        add_cfn_nag_suppressions(
            state_machine.role.node.try_find_child("DefaultPolicy").node.find_child(
                "Resource"
            ),
            [
                CfnNagSuppression(
                    "W12", "IAM policy for AWS X-Ray requires an allow on *"
                ),
                CfnNagSuppression(
                    "W76",
                    "Large step functions need larger IAM roles to access all managed AWS Lambda functions",
                ),
            ],
        )

        s3_event_handler = S3EventHandler(
            self,
            "S3EventHandler",
            state_machine=state_machine,
            bucket=data_bucket,
            layers=[layer_powertools, layer_solutions],
            topic=notifications.topic,
        )
        s3_event_notification = LambdaDestination(s3_event_handler)
        data_bucket.add_event_notification(
            EventType.OBJECT_CREATED,
            s3_event_notification,
            NotificationKeyFilter(prefix="train/", suffix=".json"),
        )

        # Handle suppressions for the notification handler resource generated by CDK
        bucket_notification_handler = self.node.try_find_child(
            "BucketNotificationsHandler050a0587b7544547bf325f094a3db834"
        )
        bucket_notification_policy = (
            bucket_notification_handler.node.find_child("Role")
            .node.find_child("DefaultPolicy")
            .node.find_child("Resource")
        )
        add_cfn_nag_suppressions(
            bucket_notification_policy,
            [
                CfnNagSuppression(
                    "W12",
                    "bucket resource is '*' due to circular dependency with bucket and role creation at the same time",
                )
            ],
        )

        cdk.Tags.of(self).add("SOLUTION_ID", self.node.try_get_context("SOLUTION_ID"))
        cdk.Tags.of(self).add(
            "SOLUTION_NAME", self.node.try_get_context("SOLUTION_NAME")
        )
        cdk.Tags.of(self).add(
            "SOLUTION_VERSION", self.node.try_get_context("SOLUTION_VERSION")
        )

        cdk.Aspects.of(self).add(
            CfnNagSuppressAll(
                suppress=[
                    CfnNagSuppression(
                        "W89",
                        "functions deployed by this solution do not require VPC access",
                    ),
                    CfnNagSuppression(
                        "W92",
                        "functions deployed by this solution do not require reserved concurrency",
                    ),
                    CfnNagSuppression(
                        "W58",
                        "functions deployed by this solution use custom policy to write to CloudWatch logs",
                    ),
                ],
                resource_type="AWS::Lambda::Function",
            )
        )

        # dashboard
        self.dashboard = Dashboard(
            self,
            "PersonalizeDashboard",
            scheduler_sfn_arn=scheduler.state_machine_arn,
            personalize_bucket_name=data_bucket.bucket_name,
        )

        # outputs
        cdk.CfnOutput(
            self,
            "PersonalizeBucketName",
            value=data_bucket.bucket_name,
            export_name=f"{Aws.STACK_NAME}-PersonalizeBucketName",
        )
        cdk.CfnOutput(
            self,
            "SchedulerTableName",
            value=scheduler.scheduler_table.table_name,
            export_name=f"{Aws.STACK_NAME}-SchedulerTableName",
        )
        cdk.CfnOutput(
            self,
            "Dashboard",
            value=self.dashboard.name,
            export_name=f"{Aws.STACK_NAME}-Dashboard",
        )
