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
import os
from uuid import UUID

import pytest
import requests

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.solutions_metrics.src.custom_resources.metrics import (
    helper,
    send_metrics,
    _sanitize_data,
)

MOCKER_METRICS_ENDPOINT = (
    "aws_solutions.cdk.aws_lambda.cfn_custom_resources.solutions_metrics.src.custom_resources.metrics.METRICS_ENDPOINT"
)
MOCKER_REQUESTS = (
    "aws_solutions.cdk.aws_lambda.cfn_custom_resources.solutions_metrics.src.custom_resources.metrics.requests"
)


@pytest.fixture(params=["Create", "Update", "Delete"])
def test_event(request):
    event = {
        "RequestType": request.param,
        "ResourceProperties": {"Solution": "SOL0123", "Version": "v1.4.5","Metric1": "Data1"},
    }
    yield event


def test_sanitize_data():
    event = {
        "RequestType": "Create",
        "ResourceProperties": {
            "ServiceToken": "REMOVEME",
            "Resource": "REMOVEME",
            "Solution": "REMOVEME",
            "UUID": "REMOVEME",
            "Keep": "Me",
        },
    }

    result = _sanitize_data(event)
    assert result == {"Keep": "Me", "CFTemplate": "Created"}


def test_send_metrics(test_event):
    test_event["ResourceProperties"]["Resource"] = "UUID"
    send_metrics(test_event, None)

    # raises a ValueError if we didn't get a uuid back
    UUID(helper.Data["UUID"], version=4)


def test_send_metrics_real(test_event, mocker):
    metrics_endpoint = os.getenv("METRICS_ENDPOINT")
    if metrics_endpoint:
        mocker.patch(
            MOCKER_METRICS_ENDPOINT,
            metrics_endpoint,
        )
        send_metrics(test_event, None)


def test_send_metrics(mocker, test_event):
    requests_mock = mocker.MagicMock()
    mock_endpoint = "https://metrics-endpoint.com/example"
    mocker.patch(MOCKER_REQUESTS, requests_mock)
    mocker.patch(
        MOCKER_METRICS_ENDPOINT,
        mock_endpoint,
    )

    result = send_metrics(test_event, None)
    assert UUID(result, version=4)

    assert requests_mock.post.call_args[0][0] == mock_endpoint

    request_data = requests_mock.post.call_args[1].get("json")
    assert request_data.get("Solution") == "SOL0123"
    assert request_data.get("UUID")
    assert request_data.get("TimeStamp")

    data = request_data.get("Data")
    assert data.get("Metric1") == "Data1"
    assert data.get("CFTemplate") in ["Created", "Deleted", "Updated"]

    headers = requests_mock.post.call_args[1].get("headers")
    assert headers.get("Content-Type") == "application/json"


def test_uuid_reuse(mocker, test_event):
    requests_mock = mocker.MagicMock()
    mocker.patch(MOCKER_REQUESTS, requests_mock)
    uuid_to_set = "b14cc738-4c6c-42eb-b39b-4506a6a76911"

    if test_event.get("RequestType") == "Create":
        # on create, we CloudFormation doesn't send a UUID
        generated_uuid = send_metrics(test_event, None)
        assert UUID(generated_uuid, version=4)
    else:
        # on update/ delete, CloudFormation sends a UUID, and the custom resource should return it as passed
        test_event["PhysicalResourceId"] = uuid_to_set
        generated_uuid = send_metrics(test_event, None)
        assert generated_uuid == uuid_to_set


def test_request_exception(mocker, test_event, caplog):
    requests_mock = mocker.MagicMock()
    mocker.patch(MOCKER_REQUESTS, requests_mock)
    requests_mock.exceptions.RequestException = requests.exceptions.RequestException
    requests_mock.post.side_effect = requests.exceptions.ConnectionError("there was a connection error")

    with caplog.at_level(logging.INFO):
        send_metrics(test_event, None)

    assert ("Could not send usage data: there was a connection error") in caplog.messages


def test_general_exception(mocker, test_event, caplog):
    requests_mock = mocker.MagicMock()
    mocker.patch(MOCKER_REQUESTS, requests_mock)
    requests_mock.exceptions.RequestException = requests.exceptions.RequestException
    requests_mock.post.side_effect = ValueError("general exception")

    with caplog.at_level(logging.INFO):
        send_metrics(test_event, None)

    assert ("Unknown error when trying to send usage data: general exception") in caplog.messages
