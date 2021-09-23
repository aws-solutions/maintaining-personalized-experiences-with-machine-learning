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
from collections import namedtuple

import pytest
from botocore.stub import Stubber

from aws_lambda.sns_notification.handler import lambda_handler
from aws_solutions.core import get_service_client

TRACE_ID = "1-57f5498f-d91047849216d0f2ea3b6442"


@pytest.fixture
def sns_stubber():
    sns_client = get_service_client("sns")
    with Stubber(sns_client) as stubber:
        yield stubber


@pytest.fixture
def trace_enabled():
    os.environ["_X_AMZN_TRACE_ID"] = TRACE_ID
    yield
    del os.environ["_X_AMZN_TRACE_ID"]


DATASET_GROUP_NAME = "DATASET_GROUP_NAME"
EXPECTED_MESSAGE = """
There was an error running the personalization job for dataset group DATASET_GROUP_NAME

Message: ERROR_MESSAGE

""".lstrip(
    "\n"
)
EXPECTED_MESSAGE_TRACE = f"""
There was an error running the personalization job for dataset group DATASET_GROUP_NAME

Message: ERROR_MESSAGE

Traces: https://console.aws.amazon.com/xray/home?region=us-east-1#/traces/{TRACE_ID}
""".strip(
    "\n"
)


@pytest.fixture
def context():
    ctx = namedtuple("Context", ["invoked_function_arn"])
    return ctx(f"arn:aws:lambda:us-east-1:{'1' * 12}:function:my-function:1")


def test_sns_notification(sns_stubber, context):
    """Test without traces"""
    sns_stubber.add_response(
        "publish",
        {},
        expected_params={
            "TopicArn": os.environ.get("SNS_TOPIC_ARN"),
            "Subject": "Maintaining Personalized Experiences with Machine Learning Notifications",
            "Message": EXPECTED_MESSAGE,
        },
    )

    lambda_handler(
        {
            "datasetGroup": DATASET_GROUP_NAME,
            "statesError": {
                "Cause": '{"errorMessage": "ERROR_MESSAGE"}',
            },
        },
        context,
    )


def test_sns_notification_trace(sns_stubber, trace_enabled, context):
    """Test with traces"""
    sns_stubber.add_response(
        "publish",
        {},
        expected_params={
            "TopicArn": os.environ.get("SNS_TOPIC_ARN"),
            "Subject": "Maintaining Personalized Experiences with Machine Learning Notifications",
            "Message": EXPECTED_MESSAGE_TRACE,
        },
    )

    lambda_handler(
        {
            "datasetGroup": DATASET_GROUP_NAME,
            "statesError": {
                "Cause": '{"errorMessage": "ERROR_MESSAGE"}',
            },
        },
        context,
    )
