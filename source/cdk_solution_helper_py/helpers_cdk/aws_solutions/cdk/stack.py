# #####################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                 #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. You may obtain a copy of the License at                                                          #
#                                                                                                                     #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                        #
#                                                                                                                     #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed   #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for  #
#  the specific language governing permissions and limitations under the License.                                     #
# #####################################################################################################################

from __future__ import annotations

import re

import jsii
from aws_cdk import Stack, Aspects, IAspect
from constructs import Construct, IConstruct

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.solutions_metrics import Metrics
from aws_solutions.cdk.interfaces import TemplateOptions
from aws_solutions.cdk.mappings import Mappings

from cdk_nag import NagSuppressions
from cdk_nag import NagPackSuppression

RE_SOLUTION_ID = re.compile(r"^SO\d+$")
RE_TEMPLATE_FILENAME = re.compile(r"^[a-z]+(?:-[a-z]+)*\.template$")  # NOSONAR


def validate_re(name, value, regex: re.Pattern):
    if regex.match(value):
        return value
    raise ValueError(f"{name} must match '{regex.pattern}")


def validate_solution_id(solution_id: str) -> str:
    return validate_re("solution_id", solution_id, RE_SOLUTION_ID)


def validate_template_filename(template_filename: str) -> str:
    return validate_re("template_filename", template_filename, RE_TEMPLATE_FILENAME)


@jsii.implements(IAspect)
class MetricsAspect:
    def __init__(self, stack: SolutionStack):
        self.stack = stack

    def visit(self, node: IConstruct):
        """Called before synthesis, this allows us to set metrics at the end of synthesis"""
        if node == self.stack:
            self.stack.metrics = Metrics(self.stack, "Metrics", self.stack.metrics)

            NagSuppressions.add_resource_suppressions(self.stack.metrics, [
                NagPackSuppression(
                    id='AwsSolutions-IAM5',
                    reason='All IAM policies defined in this solution'
                           'grant only least-privilege permissions. Wild '
                           'card for resources is used only for services '
                           'which do not have a resource arn')],
                apply_to_children=True)


class SolutionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        description: str,
        template_filename,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.metrics = {}
        self.solution_id = self.node.try_get_context("SOLUTION_ID")
        self.solution_version = self.node.try_get_context("SOLUTION_VERSION")
        self.mappings = Mappings(self, solution_id=self.solution_id)
        self.solutions_template_filename = validate_template_filename(template_filename)
        self.description = description.strip(".")
        self.solutions_template_options = TemplateOptions(
            self,
            construct_id=construct_id,
            description=f"({self.solution_id}) - {self.description}. Version {self.solution_version}",
            filename=template_filename,
        )
        Aspects.of(self).add(MetricsAspect(self))
