# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from aws_cdk import Stack
from constructs import Construct

from aws_solutions.cdk.aws_lambda.python.layer import SolutionsPythonLayerVersion


class PowertoolsLayer(SolutionsPythonLayerVersion):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        requirements_path: Path = Path(__file__).absolute().parent / "requirements"
        super().__init__(scope, construct_id, requirements_path, **kwargs)

    @staticmethod
    def get_or_create(scope: Construct, **kwargs):
        stack = Stack.of(scope)
        construct_id = "PowertoolsLayer-8E932F0F-197D-4026-A354-23D184C2A624"
        exists = stack.node.try_find_child(construct_id)
        if exists:
            return exists
        return PowertoolsLayer(stack, construct_id, **kwargs)
