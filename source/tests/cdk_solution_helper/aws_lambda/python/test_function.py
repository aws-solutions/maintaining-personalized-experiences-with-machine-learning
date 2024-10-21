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
import shutil
from pathlib import Path

import pytest
from aws_cdk import Stack, App
from constructs import Construct

from aws_solutions.cdk.aws_lambda.python.function import (
    SolutionsPythonFunction,
)
from aws_solutions.cdk.aws_lambda.python.hash_utils import DirectoryHash
from aws_solutions.cdk.helpers.copytree import copytree

PYTHON_FUNCTION_NAME = "user_python_lambda_function.py"
PYTHON_FUNCTION_HANDLER_NAME = "my_handler"
PYTHON_FUNCTION = f"""
def {PYTHON_FUNCTION_HANDLER_NAME}(event, context):
    print("Hello World!") 
"""


@pytest.fixture(params=["requirements.txt", "Pipfile", "pyproject.toml"])
def python_lambda(tmp_path, request):
    requirements = request.param

    entrypoint = tmp_path / PYTHON_FUNCTION_NAME
    entrypoint.write_text(PYTHON_FUNCTION)

    # copy lambda function
    lambda_function = Path(__file__).parent / "fixtures" / "lambda"
    copytree(lambda_function, tmp_path)

    # copy requirements
    shutil.copy(Path(__file__).parent / "fixtures" / requirements, tmp_path)

    yield entrypoint, PYTHON_FUNCTION_HANDLER_NAME, requirements


@pytest.fixture
def function_synth(python_lambda, caplog):
    entrypoint, function_name, _ = python_lambda

    class FunctionStack(Stack):
        def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
            super().__init__(scope, construct_id, **kwargs)
            func = SolutionsPythonFunction(
                self,
                "TestFunction",
                entrypoint=entrypoint,
                function=function_name,
            )
            func.node.default_child.override_logical_id("TestFunction")

    with caplog.at_level(logging.DEBUG):
        app = App()
        FunctionStack(app, "test-function")
        synth = app.synth()
        print(f"CDK synth directory: {synth.directory}")
        yield synth


def test_function_has_default_role(function_synth):
    function_stack = function_synth.get_stack_by_name("test-function").template
    func = function_stack["Resources"]["TestFunction"]
    assert func["Type"] == "AWS::Lambda::Function"
    assert func["Properties"]["Handler"] == PYTHON_FUNCTION_NAME.split(".")[0] + "." + PYTHON_FUNCTION_HANDLER_NAME
    assert func["Properties"]["Runtime"] == "python3.11"

    role = function_stack["Resources"][func["Properties"]["Role"]["Fn::GetAtt"][0]]
    assert role["Type"] == "AWS::IAM::Role"
    assert role["Properties"] == {
        "AssumeRolePolicyDocument": {
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                }
            ],
            "Version": "2012-10-17",
        },
        "Policies": [
            {
                "PolicyDocument": {
                    "Statement": [
                        {
                            "Action": [
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                            ],
                            "Effect": "Allow",
                            "Resource": {
                                "Fn::Join": [
                                    "",
                                    [
                                        "arn:",
                                        {"Ref": "AWS::Partition"},
                                        ":logs:",
                                        {"Ref": "AWS::Region"},
                                        ":",
                                        {"Ref": "AWS::AccountId"},
                                        ":log-group:/aws/lambda/*",
                                    ],
                                ]
                            },
                        }
                    ],
                    "Version": "2012-10-17",
                },
                "PolicyName": "LambdaFunctionServiceRolePolicy",
            }
        ],
    }


@pytest.mark.no_cdk_lambda_mock
def test_library_packaging(python_lambda):
    entrypoint, function_name, requirements = python_lambda
    if requirements != "requirements.txt":
        pytest.skip(f"not testing with {requirements}")

    package_dir = entrypoint.parent
    Path(entrypoint.parent / "shared").mkdir()
    Path(entrypoint.parent / "shared" / "__init__.py").touch()
    Path(entrypoint.parent / "shared" / "lib.py").touch()

    class FunctionStack(Stack):
        def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
            super().__init__(scope, construct_id, **kwargs)
            func = SolutionsPythonFunction(
                self,
                "TestFunction",
                entrypoint=entrypoint,
                function=function_name,
                libraries=Path(package_dir / "shared"),
            )
            func.node.default_child.override_logical_id("TestFunction")

    app = App()
    FunctionStack(app, "test-function")
    synth = app.synth()
    print(f"CDK synth directory: {synth.directory}")

    assert (next(Path(synth.directory).glob("asset.*")) / "shared" / "__init__.py").exists()
    assert (next(Path(synth.directory).glob("asset.*")) / "shared" / "lib.py").exists()


def test_directory_hash():
    fixture_path = Path(__file__).parent / "fixtures" / "hash_fixture"
    r1 = DirectoryHash.hash(fixture_path)
    assert r1 == "817e97ccbdadc94c102e5f193e079b91d1123a1f"  # hash of 'caz'

    r2 = DirectoryHash.hash(fixture_path, fixture_path)
    assert r2 == "964c581070520bb9726cd9a0996ebc28a4981bc6"  # hash of 'cazcaz'
