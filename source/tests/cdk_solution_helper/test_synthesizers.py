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

import os

import pytest
from aws_cdk import App, Stack
from aws_solutions.cdk.interfaces import TemplateOptions
from aws_solutions.cdk.mappings import Mappings
from aws_solutions.cdk.synthesizers import SolutionStackSubstitutions


@pytest.fixture
def solution_build_environment(monkeypatch):
    monkeypatch.setenv("SOLUTIONS_ASSETS_GLOBAL", "global-s3-assets")
    monkeypatch.setenv("SOLUTIONS_ASSETS_REGIONAL", "regional-s3-assets")
    yield
    monkeypatch.delenv("SOLUTIONS_ASSETS_GLOBAL")
    monkeypatch.delenv("SOLUTIONS_ASSETS_REGIONAL")


@pytest.fixture
def template():
    context = {
        "SOLUTION_VERSION": "v1.0.0",
        "SOLUTION_NAME": "test-solution-name",
        "BUCKET_NAME": "test-solution-bucket",
        "APP_REGISTRY_NAME": "test-solution-name",
    }
    for ctx_var in ["SOLUTIONS_ASSETS_GLOBAL", "SOLUTIONS_ASSETS_REGIONAL"]:
        ctx_var_val = os.environ.get(ctx_var)
        if ctx_var_val:
            context[ctx_var] = ctx_var_val

    synth = SolutionStackSubstitutions()
    app = App(context=context)
    stack = Stack(app, "stack-id-1", synthesizer=synth)
    stack.synthesizer.bind(stack)
    TemplateOptions(stack, "id_1", "description_1", "stack_1.template")
    Mappings(stack, "SO001")

    # SOLUTIONS_ASSETS_GLOBAL / SOLUTIONS_ASSETS_REGIONAL not set:
    # this will not remove the CDK generated parameters (this was called by CDK)
    yield app.synth().stacks[0].template



def test_cloudformation_template_init(template):
    assert template["Parameters"]
    assert template["Rules"]["CheckBootstrapVersion"]


def test_cloudformation_template_init_metadata(solution_build_environment, template):
    assert not template.get("Parameters")
    assert not template.get("Rules")

    assert template["Metadata"]["aws:solutions:templatename"] == "stack_1.template"

    assert template["Mappings"]["Solution"] == {
        "Data": {
            "ID": "SO001",
            "Version": "v1.0.0",
            "SendAnonymousUsageData": "Yes",
            "SolutionName": "test-solution-name",
            "AppRegistryName": "test-solution-name",
            "ApplicationType": "AWS-Solutions",
        }
    }
    assert template["Mappings"]["SourceCode"] == {
        "General": {"S3Bucket": "test-solution-bucket", "KeyPrefix": "test-solution-name/v1.0.0"}
    }
