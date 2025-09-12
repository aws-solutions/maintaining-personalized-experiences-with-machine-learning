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
from aws_cdk import Duration, Aws
from aws_cdk.aws_lambda import Tracing, Runtime, RuntimeFamily
from constructs import Construct

from aws_solutions.cdk.aws_lambda.environment import Environment
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from aws_solutions.cdk.cfn_guard import add_cfn_guard_suppressions


class CreateConfig(SolutionsPythonFunction):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        entrypoint = Path(__file__).absolute().parents[4] / "aws_lambda" / "create_config" / "handler.py"
        function_name = "lambda_handler"
        kwargs["libraries"] = [Path(__file__).absolute().parents[4] / "aws_lambda" / "shared"]
        kwargs["tracing"] = Tracing.ACTIVE
        kwargs["timeout"] = Duration.seconds(90)
        kwargs["runtime"] = Runtime("python3.11", RuntimeFamily.PYTHON)

        super().__init__(scope, construct_id, entrypoint, function_name, **kwargs)

        self.environment = Environment(self)

        add_cfn_nag_suppressions(
            self.role.node.try_find_child("DefaultPolicy").node.find_child("Resource"),
            [CfnNagSuppression("W12", "IAM policy for AWS X-Ray requires an allow on *")],
        )

        add_cfn_guard_suppressions(
            self.role.node.try_find_child("Resource"),
            ["IAM_NO_INLINE_POLICY_CHECK"]
        )

        self._set_permissions()

    def _set_permissions(self):
        self.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "personalize:Describe*",
                    "personalize:List*",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:*",
                ],
            )
        )
