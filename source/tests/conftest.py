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
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Optional

import boto3
import jsii
import pytest
from aws_cdk.aws_lambda import (
    FunctionProps,
    Code,
    Runtime,
    Function,
    LayerVersionProps,
    LayerVersion,
)
from botocore.stub import Stubber
from constructs import Construct

from aws_solutions.core import get_service_client

shared_path = str(Path(__file__).parent.parent / "aws_lambda")
if shared_path not in sys.path:
    sys.path.insert(0, shared_path)


from shared.notifiers.base import Notifier
from shared.resource import Resource


class Solution:
    id = "SO0170test"
    version = "99.99.99"

    @property
    def context(self):
        return {
            "SOLUTION_NAME": "Maintaining Personalized Experiences with Machine Learning",
            "SOLUTION_ID": self.id,
            "SOLUTION_VERSION": self.version,
        }


@pytest.fixture
def solution():
    return Solution()


@pytest.fixture(scope="session", autouse=True)
def solution_env():
    os.environ[
        "SNS_TOPIC_ARN"
    ] = f"arn:aws:sns:us-east-1:{'1'*12}:some-personalize-notification-topic"
    os.environ[
        "STATE_MACHINE_ARN"
    ] = f"arn:aws:states:us-east-1:{'1'*12}:stateMachine:personalize-workflow"
    os.environ[
        "EVENT_BUS_ARN"
    ] = f"arn:aws:events:us-east-1:{'1'*12}:event-bus/PersonalizeEventBus"
    yield


@pytest.fixture
def cdk_entrypoint():
    """This otherwise would not be importable (it's not in a package, and is intended to be a script)"""
    sys.path.append(str((Path(__file__).parent.parent / "infrastructure").absolute()))
    yield


@pytest.fixture
def personalize_stubber():
    personalize_client = get_service_client("personalize")
    with Stubber(personalize_client) as stubber:
        yield stubber


def mock_lambda_init(
    self,  # NOSONAR (python:S107) - allow large number of method parameters
    scope: Construct,
    id: str,
    *,
    code: Code,
    handler: str,
    runtime: Runtime,
    **kwargs,
) -> None:
    # overriding the code will prevent building lambda functions
    # override the runtime list for now, as well, to match above
    props = FunctionProps(
        code=Code.from_inline("return"),
        handler=handler,
        runtime=Runtime.PYTHON_3_7,
        **kwargs,
    )
    jsii.create(Function, self, [scope, id, props])


def mock_layer_init(self, scope: Construct, id: str, *, code: Code, **kwargs) -> None:
    # overriding the layers will prevent building lambda layers
    # override the runtime list for now, as well, to match above
    with TemporaryDirectory() as tmpdirname:
        kwargs["code"] = Code.from_asset(path=tmpdirname)
        kwargs["compatible_runtimes"] = [Runtime.PYTHON_3_7]
        props = LayerVersionProps(**kwargs)
        jsii.create(LayerVersion, self, [scope, id, props])


@pytest.fixture(autouse=True)
def cdk_lambda_mocks(mocker, request):
    """Using this session mocker means we cannot assert anything about functions or layer versions of this stack"""
    if "no_cdk_lambda_mock" in request.keywords:
        yield
    else:
        mocker.patch("aws_cdk.aws_lambda.Function.__init__", mock_lambda_init)
        mocker.patch("aws_cdk.aws_lambda.LayerVersion.__init__", mock_layer_init)
        yield


@pytest.fixture
def configuration_path():
    return Path(__file__).parent / "fixtures" / "config" / "sample_config.json"


class NotifierStub(Notifier):
    def __init__(self):
        self.creation_notifications = []
        self.completion_notifications = []

    @property
    def has_notified_for_creation(self) -> bool:
        if len(self.creation_notifications) > 1:
            raise ValueError("should not notify for creation more than once")
        return len(self.creation_notifications) == 1

    @property
    def has_notified_for_complete(self) -> bool:
        if len(self.completion_notifications) > 1:
            raise ValueError("should not notify for completion more than once")
        return len(self.completion_notifications) == 1

    @property
    def latest_notification_status(self):
        if self.has_notified_for_complete and self.has_notified_for_creation:
            raise ValueError("should not notifiy for both creation and completion")

        if self.has_notified_for_creation:
            status = self.creation_notifications[0]["status"]
        elif self.has_notified_for_complete:
            status = self.completion_notifications[0]["status"]
        else:
            raise ValueError("no notifications have been requested")

        return status

    def notify_create(self, status: str, resource: Resource, result: Dict):
        self.creation_notifications.append(
            {
                "resource": resource.name.camel,
                "result": result,
                "status": status,
            }
        )

    def notify_complete(self, status: str, resource: Resource, result: Dict):
        self.completion_notifications.append(
            {
                "resource": resource.name.camel,
                "result": result,
                "status": status,
            }
        )


@pytest.fixture(scope="function")
def notifier_stubber(mocker):
    notifier = NotifierStub()
    mocker.patch("shared.events.NOTIFY_LIST", [notifier])
    yield notifier


@pytest.fixture
def validate_handler_config():
    """Validates a handler configuration against the installed botocore shapes"""

    def _validate_handler_config(
        resource: str, config: Dict, status: Optional[str] = None
    ):
        cli = boto3.client("personalize")

        shape = resource[0].upper() + resource[1:]
        request_shape = cli.meta.service_model.shape_for(f"Create{shape}Request")
        response_shape = cli.meta.service_model.shape_for(f"Describe{shape}Response")

        # check config parameter
        for k in config.keys():
            if "workflowConfig" not in config[k].get("path"):
                assert (
                    k in request_shape.members.keys()
                ), f"invalid key {k} not in Create{shape} API call"

        for k in request_shape.members.keys():
            assert k in config.keys(), "missing {k} in config"

        # check status parameter
        if status:
            m = response_shape
            for k in status.split("."):
                assert (
                    k in m.members.keys()
                ), f"status component {k} not found in {m.keys()}"
                m = m.members[k]

    return _validate_handler_config
