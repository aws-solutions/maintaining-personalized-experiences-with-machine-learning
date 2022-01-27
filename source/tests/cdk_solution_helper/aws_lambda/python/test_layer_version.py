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
import json
import logging
import shutil
from pathlib import Path

import pytest
from aws_cdk import Stack, App
from constructs import Construct

from aws_solutions.cdk.aws_lambda.python.layer import SolutionsPythonLayerVersion
from aws_solutions.cdk.helpers.copytree import copytree


@pytest.fixture(params=["requirements.txt"])
def python_layer_dir(tmp_path, request):
    requirements = request.param

    entrypoint = tmp_path

    # copy lambda function
    lambda_function = Path(__file__).parent / "fixtures" / "lambda"
    copytree(lambda_function, tmp_path)

    # copy requirements
    shutil.copy(Path(__file__).parent / "fixtures" / requirements, tmp_path)

    yield entrypoint


@pytest.fixture
def layer_synth(python_layer_dir, caplog):
    source_path = python_layer_dir

    class LayerVersionStack(Stack):
        def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
            super().__init__(scope, construct_id, **kwargs)
            func = SolutionsPythonLayerVersion(
                self,
                "TestLayerVersion",
                requirements_path=source_path,
            )
            func.node.default_child.override_logical_id("TestLayerVersion")

    with caplog.at_level(logging.DEBUG):
        app = App()
        LayerVersionStack(app, "test-layer-version")
        synth = app.synth()
        print(f"CDK synth directory: {synth.directory}")
        yield synth


@pytest.mark.no_cdk_lambda_mock
def test_layer_version(layer_synth):
    layer_synth.get_stack_by_name("test-layer-version").template
    directory = Path(layer_synth.directory)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))

    asset_file = manifest["artifacts"]["test-layer-version.assets"]["properties"][
        "file"
    ]
    assets = json.loads((directory / asset_file).read_text(encoding="utf-8"))
    asset_dir = next(
        iter(
            [
                v
                for v in assets["files"].values()
                if v.get("source", {}).get("packaging") == "zip"
            ]
        )
    )["source"]["path"]
    asset_path = directory / asset_dir

    # check that the package was installed to the correct path
    assert (asset_path / "python" / "minimal").exists()
