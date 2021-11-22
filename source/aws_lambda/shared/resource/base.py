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
