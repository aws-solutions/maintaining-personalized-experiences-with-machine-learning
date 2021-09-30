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

import re

import pytest
from aws_cdk.core import App, CfnParameter

from aws_solutions.cdk.stack import (
    SolutionStack,
    validate_re,
    validate_solution_id,
    validate_template_filename,
)


@pytest.mark.parametrize(
    "valid_solution_id",
    [
        "SO0",
        "SO0",
        "SO000",
    ],
)
def test_validate_solution_id_valid(valid_solution_id):
    assert validate_solution_id(valid_solution_id) == valid_solution_id


@pytest.mark.parametrize(
    "invalid_solution_id",
    [
        "S00",
        "SO0a",
        "SO000b",
    ],
)
def test_validate_solution_id_invalid(invalid_solution_id):
    with pytest.raises(ValueError):
        validate_solution_id(invalid_solution_id)


@pytest.mark.parametrize(
    "valid_template_filename",
    [
        "solution.template",
        "solution-detail.template",
    ],
)
def test_validate_template_filename_valid(valid_template_filename):
    assert (
        validate_template_filename(valid_template_filename) == valid_template_filename
    )


@pytest.mark.parametrize(
    "invalid_template_filename",
    [
        "SOLUTION.template",
        "solution.TEMPLATE",
        "solution_detail.template",
        "solution-.template",
    ],
)
def test_validate_template_filename_invalid(invalid_template_filename):
    with pytest.raises(ValueError):
        validate_template_filename(invalid_template_filename)


def test_validate_re():
    regex = re.compile(r"\d")
    assert validate_re("some", "1", regex) == "1"


def test_validate_re_exception():
    regex = re.compile(r"\d")
    with pytest.raises(ValueError):
        assert validate_re("some", "a", regex)


def test_solution_stack():
    stack_id = "S00123"
    stack_description = "stack description"
    stack_filename = "stack-name.template"

    app = App(context={"SOLUTION_ID": stack_id})
    SolutionStack(app, "stack", stack_description, stack_filename)

    template = app.synth().stacks[0].template

    assert template["Description"] == f"({stack_id}) {stack_description}"
    assert template["Metadata"] == {
        "AWS::CloudFormation::Interface": {
            "ParameterGroups": [],
            "ParameterLabels": {},
        },
        "aws:solutions:templatename": "stack-name.template",
    }
    assert template["Conditions"] == {
        "SendAnonymousUsageData": {
            "Fn::Equals": [
                {"Fn::FindInMap": ["Solution", "Data", "SendAnonymousUsageData"]},
                "Yes",
            ]
        }
    }


@pytest.mark.parametrize("execution_number", range(5))
def test_stack_parameter_ordering(execution_number):
    app = App(context={"SOLUTION_ID": "SO0123"})
    stack = SolutionStack(app, "stack", "test stack", "test-stack.template")

    param_1 = CfnParameter(stack, "parameter1")
    param_2 = CfnParameter(stack, "parameter2")

    stack.solutions_template_options.add_parameter(param_1, "parameter 1", "group 1")
    stack.solutions_template_options.add_parameter(param_2, "parameter 2", "group 2")

    template = app.synth().stacks[0].template

    assert (
        template["Metadata"]["AWS::CloudFormation::Interface"]["ParameterGroups"][0][
            "Label"
        ]["default"]
        == "group 1"
    )
    assert template["Metadata"]["AWS::CloudFormation::Interface"]["ParameterGroups"][0][
        "Parameters"
    ] == ["parameter1"]
    assert (
        template["Metadata"]["AWS::CloudFormation::Interface"]["ParameterLabels"][
            "parameter1"
        ]["default"]
        == "parameter 1"
    )
    assert (
        template["Metadata"]["AWS::CloudFormation::Interface"]["ParameterLabels"][
            "parameter2"
        ]["default"]
        == "parameter 2"
    )
