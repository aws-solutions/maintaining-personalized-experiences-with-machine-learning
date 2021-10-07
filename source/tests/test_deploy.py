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

import sys
from pathlib import Path

import pytest


@pytest.fixture
def cdk_entrypoint():
    """This otherwise would not be importable (it's not in a package, and is intended to be a script)"""
    sys.path.append(str((Path(__file__).parent.parent / "infrastructure").absolute()))
    yield


def test_deploy(solution, cdk_entrypoint):
    from deploy import build_app, solution as cdk_solution

    cdk_solution.reset()

    extra_context = "EXTRA_CONTEXT"
    source_bucket = "SOURCE_BUCKET"
    synth = build_app({extra_context: extra_context, "BUCKET_NAME": source_bucket})
    stack = synth.get_stack("PersonalizeStack")
    assert solution.id in stack.template["Description"]
    assert (
        source_bucket == stack.template["Mappings"]["SourceCode"]["General"]["S3Bucket"]
    )
    assert solution.id == stack.template["Mappings"]["Solution"]["Data"]["ID"]
    assert (
        "Yes"
        == stack.template["Mappings"]["Solution"]["Data"]["SendAnonymousUsageData"]
    )
    assert stack.template["Outputs"]["PersonalizeBucketName"]
    assert stack.template["Outputs"]["SchedulerTableName"]
    assert stack.template["Outputs"]["SNSTopicArn"]


def test_parameters(solution, cdk_entrypoint):
    """Ensure parameter ordering is kept"""
    from deploy import build_app, solution as cdk_solution

    cdk_solution.reset()

    extra_context = "EXTRA_CONTEXT"
    source_bucket = "SOURCE_BUCKET"
    synth = build_app({extra_context: extra_context, "BUCKET_NAME": source_bucket})
    stack = synth.get_stack("PersonalizeStack").template

    assert (
        stack["Metadata"]["AWS::CloudFormation::Interface"]["ParameterGroups"][0][
            "Label"
        ]["default"]
        == "Solution Configuration"
    )
    assert stack["Metadata"]["AWS::CloudFormation::Interface"]["ParameterGroups"][0][
        "Parameters"
    ] == ["Email"]
    assert (
        stack["Metadata"]["AWS::CloudFormation::Interface"]["ParameterLabels"]["Email"][
            "default"
        ]
        == "Email"
    )
    assert (
        stack["Metadata"]["AWS::CloudFormation::Interface"]["ParameterLabels"][
            "PersonalizeKmsKeyArn"
        ]["default"]
        == "(Optional) KMS key ARN used to encrypt Datasets managed by Amazon Personalize"
    )
