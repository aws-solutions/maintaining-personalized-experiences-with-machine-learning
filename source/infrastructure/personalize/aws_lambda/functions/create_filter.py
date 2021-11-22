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
from typing import Optional

import aws_cdk.aws_iam as iam
from aws_cdk.aws_stepfunctions import IChainable
from aws_cdk.core import Construct, Aws

from aws_solutions.cdk.stepfunctions.solutionstep import SolutionStep


class CreateFilter(SolutionStep):
    def __init__(
        self,
        scope: Construct,
        id: str,
        layers=None,
        failure_state: Optional[IChainable] = None,
    ):
        super().__init__(
            scope,
            id,
            layers=layers,
            failure_state=failure_state,
            entrypoint=(
                Path(__file__).absolute().parents[4]
                / "aws_lambda"
                / "create_filter"
                / "handler.py"
            ),
            libraries=[Path(__file__).absolute().parents[4] / "aws_lambda" / "shared"],
        )

    def _set_permissions(self):
        self.function.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "personalize:DescribeDatasetGroup",
                    "personalize:CreateFilter",
                    "personalize:DescribeFilter",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:filter/*",
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:dataset-group/*",
                ],
            )
        )
