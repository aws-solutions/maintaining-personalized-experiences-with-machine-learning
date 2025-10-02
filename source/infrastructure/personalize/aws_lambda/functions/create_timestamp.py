# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from constructs import Construct

from aws_solutions.cdk.stepfunctions.solutionstep import SolutionStep
from aws_solutions.cdk.cfn_guard import add_cfn_guard_suppressions


class CreateTimestamp(SolutionStep):
    def __init__(
        self,
        scope: Construct,
        id: str,
        layers=None,
    ):
        super().__init__(
            scope,
            id,
            layers=layers,
            entrypoint=(Path(__file__).absolute().parents[4] / "aws_lambda" / "create_timestamp" / "handler.py"),
        )

        add_cfn_guard_suppressions(
         self.function.role.node.try_find_child("Resource"),
          ["IAM_NO_INLINE_POLICY_CHECK"]
        )

    def _set_permissions(self):
        pass  # NOSONAR (python:S1186) - no permissions required
