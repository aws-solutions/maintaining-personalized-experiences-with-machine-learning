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
from typing import Optional

import aws_cdk.aws_cloudwatch as cw
from aws_cdk import Aws
from constructs import Construct

GREEN = "#32cd32"
RED = "#ff4500"
BLUE = "#4682b4"

QUICK_LINKS = """
# Quick Links

| Link | Description | 
|-|-|
|[button:primary:Personalize](https://console.aws.amazon.com/personalize/home?region={region}#datasetGroups)|Check the status of your managed resources in Amazon Personalize|
|[button:primary:S3](https://s3.console.aws.amazon.com/s3/buckets/{personalize_bucket_name}?region={region}&tab=objects)|Upload your workflow configuration and personalization data to S3 to trigger workflows|
|[button:primary:Scheduler](https://console.aws.amazon.com/states/home?region={region}#/statemachines/view/{scheduler_sfn_arn}?statusFilter=RUNNING)|Check out the running scheduler jobs for your personalization workflow |
"""


class Dashboard(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        scheduler_sfn_arn: str,
        personalize_bucket_name: str,
    ):
        super().__init__(scope, id)

        self.dashboard = cw.Dashboard(
            self,
            "PersonalizeDashboard",
            dashboard_name=f"PersonalizeSolution-{Aws.STACK_NAME}-{Aws.REGION}",
            period_override=cw.PeriodOverride.AUTO,
            start="-PT1D",
        )
        self.dashboard.add_widgets(
            cw.Row(
                cw.Column(
                    cw.SingleValueWidget(
                        title="Personalization Configurations Processed",
                        metrics=[
                            self._metric("ConfigurationsProcessed", "Processed", BLUE),
                            self._metric(
                                "ConfigurationsProcessedSuccesses", "Succeeded", GREEN
                            ),
                            self._metric(
                                "ConfigurationsProcessedFailures", "Failures", RED
                            ),
                        ],
                        set_period_to_time_range=True,
                        width=12,
                        height=3,
                    ),
                    cw.SingleValueWidget(
                        title="Personalization Workflow Status",
                        metrics=[
                            self._metric(
                                "JobSuccess", "Workflow Jobs Succeeded", GREEN
                            ),
                            self._metric("JobFailure", "Workflow Jobs Failed", RED),
                            self._metric(
                                "JobsCreated",
                                "Scheduler Jobs Created",
                                GREEN,
                                service="Scheduler",
                            ),
                            self._metric(
                                "JobsDeleted",
                                "Scheduler Jobs Deleted",
                                RED,
                                service="Scheduler",
                            ),
                        ],
                        set_period_to_time_range=True,
                        width=12,
                        height=3,
                    ),
                    cw.SingleValueWidget(
                        title="Amazon Personalize Resources Created",
                        metrics=[
                            self._metric(
                                "DatasetGroupCreated", "Dataset Groups Created"
                            ),
                            self._metric("DatasetCreated", "Datasets Created"),
                            self._metric(
                                "EventTrackerCreated", "Event Trackers Created"
                            ),
                            self._metric("SolutionCreated", "Solutions Created"),
                            self._metric(
                                "SolutionVersionCreated", "Solution Versions Created"
                            ),
                            self._metric("CampaignCreated", "Campaigns Created"),
                            self._metric(
                                "BatchInferenceJobCreated",
                                "Batch Inference Jobs Created",
                            ),
                            self._metric(
                                "BatchSegmentJobCreated",
                                "Batch Segment Jobs Created",
                            ),
                            self._metric("RecommenderCreated", "Recommenders Created"),
                            self._metric("FilterCreated", "Filters Created"),
                        ],
                        set_period_to_time_range=True,
                        width=12,
                        height=9,
                    ),
                ),
                cw.Column(
                    cw.TextWidget(
                        markdown=QUICK_LINKS.format(
                            region=Aws.REGION,
                            personalize_bucket_name=personalize_bucket_name,
                            scheduler_sfn_arn=scheduler_sfn_arn,
                        ),
                        height=6,
                        width=6,
                    )
                ),
            )
        )

    def _metric(
        self, name: str, label: str, color: Optional[str] = None, service="Workflow"
    ) -> cw.Metric:
        return cw.Metric(
            namespace=f"personalize_solution_{Aws.STACK_NAME}",
            metric_name=name,
            dimensions_map={"service": service},
            label=label,
            statistic="Sum",
            color=color,
        )

    @property
    def name(self) -> str:
        return self.dashboard.node.default_child.ref
