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
from pathlib import Path

import aws_cdk.aws_iam as iam
from aws_cdk import Aws
from aws_cdk.aws_s3 import IBucket
from constructs import Construct

from cdk_nag import NagSuppressions
from cdk_nag import NagPackSuppression

from aws_solutions.cdk.stepfunctions.solutionstep import SolutionStep
from aws_solutions.cdk.cfn_guard import add_cfn_guard_suppressions

class CreateBatchSegmentJob(SolutionStep):
    def __init__(
        self,
        scope: Construct,
        id: str,
        personalize_bucket: IBucket,
        layers=None,
    ):
        self.personalize_bucket = personalize_bucket
        self.personalize_batch_inference_rw_role = iam.Role(
            scope,
            "PersonalizeSegmentS3ReadWriteRole",
            description="Grants Amazon Personalize access to read/write to S3 for batch segment jobs",
            assumed_by=iam.ServicePrincipal("personalize.amazonaws.com"),
            inline_policies={
                "PersonalizeS3ReadPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
                            resources=[
                                personalize_bucket.arn_for_objects("batch/*"),
                                personalize_bucket.bucket_arn,
                            ],
                        )
                    ]
                )
            },
        )
        personalize_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
                resources=[
                    personalize_bucket.arn_for_objects("*"),
                    personalize_bucket.bucket_arn,
                ],
                principals=[iam.ServicePrincipal("personalize.amazonaws.com")],
            )
        )

        NagSuppressions.add_resource_suppressions(
            self.personalize_batch_inference_rw_role, [
                NagPackSuppression(
                    id='AwsSolutions-IAM5',
                    reason='All IAM policies defined in this solution'
                           'grant only least-privilege permissions. Wild '
                           'card for resources is used only for services '
                           'which do not have a resource arn')],
            apply_to_children=True)

        add_cfn_guard_suppressions(
         self.personalize_batch_inference_rw_role.node.try_find_child("Resource"),
          ["IAM_NO_INLINE_POLICY_CHECK"]
        )

        super().__init__(
            scope,
            id,
            layers=layers,
            entrypoint=(
                Path(__file__).absolute().parents[4] / "aws_lambda" / "create_batch_segment_job" / "handler.py"
            ),
            libraries=[Path(__file__).absolute().parents[4] / "aws_lambda" / "shared"],
        )

        add_cfn_guard_suppressions(
         self.function.role.node.try_find_child("Resource"),
          ["IAM_NO_INLINE_POLICY_CHECK"]
        )
        
    def _set_permissions(self):
        # personalize resource permissions
        self.function.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "personalize:DescribeDatasetGroup",
                    "personalize:ListBatchSegmentJobs",
                    "personalize:ListSolutionVersions",
                    "personalize:ListSolutions",
                    "personalize:CreateBatchSegmentJob",
                    "personalize:DescribeBatchSegmentJob",
                    "personalize:DescribeSolution",
                    "personalize:DescribeSolutionVersion",
                    "personalize:TagResource",
                    "personalize:ListTagsForResource",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:dataset-group/*",
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:batch-segment-job/*",
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:solution/*",
                ],
            )
        )
        self.personalize_bucket.grant_read_write(self.function, "batch/*")

        # passrole permissions
        self.function.add_to_role_policy(
            statement=iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["iam:PassRole"],
                resources=[self.personalize_batch_inference_rw_role.role_arn],
            )
        )
        self.function.add_environment("ROLE_ARN", self.personalize_batch_inference_rw_role.role_arn)
