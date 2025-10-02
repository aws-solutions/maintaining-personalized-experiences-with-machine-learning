# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
