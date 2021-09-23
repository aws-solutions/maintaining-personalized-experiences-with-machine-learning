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
from os import environ

import pytest

from aws_solutions.cdk.context import SolutionContext

SOLUTION_NAME = "aws-solution-name"
SOLUTION_VERSION = "1.0.0"
SOLUTION_CDK_SCRIPT = "deploy.py"
CDK_JSON_TEXT = json.dumps(
    {
        "app": f"python3 {SOLUTION_CDK_SCRIPT}",
        "context": {
            "SOLUTION_NAME": SOLUTION_NAME,
            "SOLUTION_VERSION": SOLUTION_VERSION,
            "@aws-cdk/core:newStyleStackSynthesis": "true",
        },
    }
)


@pytest.fixture
def cdk_json_path(tmp_path):
    d = tmp_path / "cdk_dir"
    d.mkdir()
    p = d / "cdk.json"
    p.write_text(CDK_JSON_TEXT)
    yield p


def test_aws_solution_too_many_params():
    context = SolutionContext()

    @context.requires("SOLUTION_NAME")
    def func_under_test(context, something_else):
        pass  # NOSONAR (python:S1186) - testing requires a function to annotate

    with pytest.raises(ValueError):
        func_under_test("one", "two")


def test_aws_solution_wrong_param_type():
    context = SolutionContext()

    @context.requires("SOLUTION_NAME")
    def func_under_test(context):
        pass  # NOSONAR (python:S1186) -  testing requires a function to annotate

    with pytest.raises(TypeError):
        func_under_test("one")


def test_aws_solution(cdk_json_path):
    context = SolutionContext(cdk_json_path=cdk_json_path)
    override = "overridden context"
    solution_name = environ.get(
        "SOLUTION_NAME", SOLUTION_NAME
    )  # environment variable always wins
    version = environ.get(
        "SOLUTION_VERSION", "1.2.3"
    )  # environment variable always wins

    @context.requires("SOLUTION_NAME")
    @context.requires("SOLUTION_VERSION", version)
    def func_under_test(context):
        assert context["SOLUTION_NAME"] == solution_name
        assert context["SOLUTION_VERSION"] == version
        assert context["OVERRIDE"] == override

    func_under_test({"OVERRIDE": override})


def test_aws_solution_env_vars(cdk_json_path):
    context = SolutionContext(cdk_json_path)

    override = "overridden context"
    solution_name_env = "from environment solution name"
    solution_version_env = "from environment solution version"
    environ["_SOLUTION_NAME"] = solution_name_env
    environ["_SOLUTION_VERSION"] = solution_version_env

    @context.requires("_SOLUTION_NAME")
    @context.requires("_SOLUTION_VERSION")
    def func_under_test(context):
        assert context["_SOLUTION_NAME"] == solution_name_env
        assert context["_SOLUTION_VERSION"] == solution_version_env
        assert context["_OVERRIDE"] == override

    func_under_test({"_OVERRIDE": override})

    del environ["_SOLUTION_NAME"]
    del environ["_SOLUTION_VERSION"]


def test_aws_solution_missing_context(cdk_json_path):
    context = SolutionContext(cdk_json_path)

    @context.requires("NOT_PRESENT")
    def func_under_test():
        pass  # NOSONAR (python:S1186) - testing requires a function to annotate

    with pytest.raises(ValueError):
        func_under_test()
