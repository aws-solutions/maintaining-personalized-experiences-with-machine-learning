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

import json
from pathlib import Path

import pytest
from aws_cdk import App, Stack, NestedStack, CfnParameter

from aws_solutions.cdk.interfaces import (
    _TemplateParameter,
    TemplateOptions,
    TemplateOptionsException,
)


@pytest.fixture
def stacks():
    app = App()
    stack = Stack(app, "stack-id-1")
    nested_stack = Stack(stack, "stack-id-2")
    nested_nestedstack = NestedStack(stack, "stack-id-3")

    TemplateOptions(stack, "id_1", "description_1", "stack_1.template")
    TemplateOptions(nested_stack, "id_2", "description_2", "stack_2.template")
    TemplateOptions(nested_nestedstack, "id_3", "description_3", "stack_3.template")

    synth = app.synth()
    stacks = [json.loads(path.read_text()) for path in Path(synth.directory).glob("*.template.json")]

    return stacks


def test_template_parameter():
    tp = _TemplateParameter("name", "label", "group")
    assert tp.name == "name"
    assert tp.label == "label"
    assert tp.group == "group"


def test_template_options(stacks):
    # stack 1 should reference stack 3 (NestedStack)
    # stack 2 should be independent of the others

    for stack in stacks:
        stack_description = stack["Description"]
        stack_template_name = stack["Metadata"]["aws:solutions:templatename"]

        assert stack_description.split("_")[-1] == stack_template_name.split("_")[-1].split(".")[0]

        # the only stack with resources will point to the nested stack
        if stack.get("Resources"):
            assert list(stack["Resources"].values())[0]["Metadata"]["aws:solutions:templateid"] == "id_3"
            assert list(stack["Resources"].values())[0]["Metadata"]["aws:solutions:templatename"] == "stack_3.template"
            assert len(stack["Resources"]) == 1


def test_template_suffix():
    app = App()
    stack = Stack(app, "stack-id-1")

    with pytest.raises(TemplateOptionsException):
        TemplateOptions(stack, "id_1", "description_1", "stack_1.json")


def test_template_options_add_parameters():
    app = App()
    stack = Stack(app, "stack-id-1")
    template_options = TemplateOptions(stack, "id_1", "description_1", "stack_1.template")
    template_options.add_parameter(
        parameter=CfnParameter(stack, "parameter_1"),
        label="parameter label 1",
        group="group a",  # NOSONAR (python:S1192) - string for clarity
    )
    template_options.add_parameter(
        parameter=CfnParameter(stack, "parameter_2"),
        label="parameter label 2",
        group="group b",
    )
    template_options.add_parameter(
        parameter=CfnParameter(stack, "parameter_3"),
        label="parameter label 3",
        group="group a",  # NOSONAR (python:S1192) - string for clarity
    )

    template = app.synth().stacks[0].template

    parameter_groups = template["Metadata"]["AWS::CloudFormation::Interface"]["ParameterGroups"]
    parameter_labels = template["Metadata"]["AWS::CloudFormation::Interface"]["ParameterLabels"]

    assert len(parameter_groups) == 2
    assert {
        "Label": {"default": "group a"},  # NOSONAR (python:S1192) - string for clarity
        "Parameters": ["parameter1", "parameter3"],
    } in parameter_groups
    assert {
        "Label": {"default": "group b"},
        "Parameters": ["parameter2"],
    } in parameter_groups
    assert len(parameter_labels) == 3
    assert parameter_labels["parameter1"] == {"default": "parameter label 1"}
    assert parameter_labels["parameter2"] == {"default": "parameter label 2"}
    assert parameter_labels["parameter3"] == {"default": "parameter label 3"}
