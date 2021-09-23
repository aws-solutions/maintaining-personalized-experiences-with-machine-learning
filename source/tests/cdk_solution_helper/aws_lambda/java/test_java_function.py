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
import logging
from pathlib import Path

import pytest
from aws_cdk.core import (
    Stack,
    Construct,
    App,
)

from aws_solutions.cdk.aws_lambda.java.function import SolutionsJavaFunction


@pytest.fixture
def java_function_synth(caplog):
    class FunctionStack(Stack):
        def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
            super().__init__(scope, construct_id, **kwargs)

            project_path = Path(__file__).parent.resolve() / "fixtures" / "java_sample"
            distribution_path = project_path / "build" / "distributions"

            func = SolutionsJavaFunction(
                self,
                "TestFunction",
                project_path=project_path,
                distribution_path=distribution_path,
                gradle_task="packageFat",
                gradle_test="test",
                handler="example.Handler",
            )
            func.node.default_child.override_logical_id("TestFunction")

    with caplog.at_level(logging.DEBUG):
        app = App()
        FunctionStack(app, "test-function-lambda")
        synth = app.synth()
        print(f"CDK synth directory: {synth.directory}")
        yield synth


@pytest.mark.no_cdk_lambda_mock
def test_java_function_synth(java_function_synth):
    function_stack = java_function_synth.get_stack("test-function-lambda").template
    func = function_stack["Resources"]["TestFunction"]

    assert func["Type"] == "AWS::Lambda::Function"
    assert func["Properties"]["Runtime"] == "java11"
