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

import aws_cdk.aws_iam as iam
from aws_cdk.aws_s3 import IBucket
from aws_cdk.aws_stepfunctions import IChainable
from aws_cdk.core import Construct, Aws

from personalize.aws_lambda.functions.solutionstep import SolutionStep


class CreateDatasetImportJob(SolutionStep):
    def __init__(
        self,
        scope: Construct,
        id: str,
        personalize_bucket: IBucket,
        layers=None,
        failure_state: Optional[IChainable] = None,
    ):
        self.personalize_bucket = personalize_bucket
        self.personalize_role = iam.Role(
            scope,
            "PersonalizeS3ReadRole",
            description="Grants Amazon Personalize access to read from S3",
            assumed_by=iam.ServicePrincipal("personalize.amazonaws.com"),
            inline_policies={
                "PersonalizeS3ReadPolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:GetObject",
                                "s3:ListBucket",
                            ],
                            resources=[
                                personalize_bucket.arn_for_objects("*"),
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
                actions=[
                    "s3:GetObject",
                    "s3:ListBucket",
                ],
                resources=[
                    personalize_bucket.arn_for_objects("*"),
                    personalize_bucket.bucket_arn,
                ],
                principals=[iam.ServicePrincipal("personalize.amazonaws.com")],
            )
        )

        super().__init__(
            scope,
            id,
            layers=layers,
            failure_state=failure_state,
        )

    def _set_permissions(self):
        # personalize resource permissions
        self.function.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "personalize:DescribeDatasetGroup",
                    "personalize:DescribeSchema",
                    "personalize:DescribeDataset",
                    "personalize:CreateDatasetImportJob",
                    "personalize:DescribeDatasetImportJob",
                    "personalize:ListDatasetImportJobs",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:dataset-group/*",
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:schema/*",
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:dataset/*",
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:dataset-import-job/*",
                ],
            )
        )
        self.personalize_bucket.grant_read(self.function, "train/*")

        # passrole permissions
        self.function.add_to_role_policy(
            statement=iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["iam:PassRole"],
                resources=[self.personalize_role.role_arn],
            )
        )
        self.function.add_environment("ROLE_ARN", self.personalize_role.role_arn)
