# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass, field

from aws_cdk.aws_lambda import IFunction


@dataclass
class EnvironmentVariable:
    scope: IFunction
    name: str
    value: str = field(default="")

    def __post_init__(self):
        if not self.value:
            self.value = self.scope.node.try_get_context(self.name)
        self.scope.add_environment(self.name, self.value)

    def __str__(self):
        return self.value
