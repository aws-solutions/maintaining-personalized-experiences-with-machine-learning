# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import List

from aws_solutions.core import get_aws_partition, get_aws_region, get_aws_account
from shared.resource.name import ResourceName


class Resource:
    children: List[Resource] = []
    has_soft_limit: bool = False

    def __init__(self):
        name = self.__class__.__name__
        name = name[0].lower() + name[1:]
        self.name = ResourceName(name)

    def arn(self, name: str, **kwargs) -> str:
        if self.name.camel == "solutionVersion":
            arn_prefix = f"arn:{get_aws_partition()}:personalize:{get_aws_region()}:{get_aws_account()}"
            return f"{arn_prefix}:solution/{name}/{kwargs.get('sv_id', 'unknown')}"
        else:
            arn_prefix = f"arn:{get_aws_partition()}:personalize:{get_aws_region()}:{get_aws_account()}"
            return f"{arn_prefix}:{self.name.dash}/{name}"

    def __eq__(self, other):
        return self.name.camel == other.name.camel

    def __hash__(self):
        return hash(self.name.camel)
