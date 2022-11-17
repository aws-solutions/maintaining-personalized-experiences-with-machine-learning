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
from pathlib import Path

import pytest

from aws_solutions.cdk.helpers.loader import load_cdk_app, CDKLoaderException

CDK_APP = """
from constructs import Construct
from aws_cdk import App, Stack

class EmptyStack(Stack): 
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id)

def cdk():        
    app = App()
    stack = EmptyStack(app, 'empty-stack')
    return app.synth() 
"""

CDK_APP_BAD = """
hey, this isn't valid python! 
"""

CDK_JSON = """
{
  "app": "python3 deploy.py",
  "context": {}
}
"""

CDK_JSON_BAD = """
{ this is not json } 
"""

CDK_JSON_MISSING_APP = """
{
  "context": {}
}
"""

CDK_JSON_MISSING_PYTHON3 = """
{
  "app": "node index",
  "context": {}
}
"""


@pytest.fixture
def cdk_app(tmp_path):
    deploy_py = Path(tmp_path / "deploy.py")
    cdk_json = Path(tmp_path / "cdk.json")

    deploy_py.write_text(CDK_APP)
    cdk_json.write_text(CDK_JSON)

    yield (tmp_path, deploy_py, cdk_json)


@pytest.fixture
def cdk_app_bad(tmp_path):
    deploy_py = Path(tmp_path / "deploy.py")
    cdk_json = Path(tmp_path / "cdk.json")

    yield (tmp_path, deploy_py, cdk_json)


def test_load_cdk_app(cdk_app):
    _, deploy_py, _ = cdk_app
    cdk_entrypoint = load_cdk_app(deploy_py, "deploy:cdk")

    assert cdk_entrypoint.__name__ == "cdk"
    assert callable(cdk_entrypoint)
    result = cdk_entrypoint()

    stack = result.get_stack_by_name("empty-stack")

    # CDK will include the bootstrap version Parameter and CheckBootstrapVersion Rule
    assert not stack.template.get("Metadata")
    assert not stack.template.get("Description")
    assert not stack.template.get("Mappings")
    assert not stack.template.get("Conditions")
    assert not stack.template.get("Transform")
    assert not stack.template.get("Resources")
    assert not stack.template.get("Outputs")


@pytest.mark.parametrize(
    "deploy_py_content, cdk_json_content, entrypoint",
    [
        (CDK_APP_BAD, CDK_JSON, "deploy:cdk"),
        (CDK_APP, CDK_JSON_BAD, "deploy:cdk"),
        (CDK_APP, CDK_JSON, "deploy"),
        (CDK_APP, CDK_JSON, "deploy.something_else:invalid"),
        (None, CDK_JSON, "deploy:cdk"),
        (CDK_APP, None, "deploy:cdk"),
        (CDK_APP, CDK_JSON_MISSING_APP, "deploy:cdk"),
        (CDK_APP, CDK_JSON_MISSING_PYTHON3, "deploy:cdk"),
    ],
    ids=[
        "bad_app",
        "bad_json",
        "bad_entrypoint",
        "worse_entrypoint",
        "missing_app",
        "missing_json",
        "missing_app",
        "missing_python3",
    ],
)
def test_load_cdk_app_invalid(cdk_app_bad, deploy_py_content, cdk_json_content, entrypoint):
    tmp_path, deploy_py, cdk_json = cdk_app_bad

    if deploy_py_content:
        deploy_py.write_text(deploy_py_content)
    else:
        try:
            deploy_py.unlink()
        except FileNotFoundError:
            pass
    if cdk_json_content:
        cdk_json.write_text(cdk_json_content)
    else:
        try:
            cdk_json.unlink()
        except FileNotFoundError:
            pass

    with pytest.raises(CDKLoaderException):
        load_cdk_app(deploy_py, entrypoint)
