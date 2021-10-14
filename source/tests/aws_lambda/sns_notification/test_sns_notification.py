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
import os
from collections import namedtuple

import boto3
import pytest
from moto import mock_sns, mock_sqs

from aws_lambda.sns_notification.handler import lambda_handler

TRACE_ID = "1-57f5498f-d91047849216d0f2ea3b6442"


@pytest.fixture
def sqs_mock():
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    topic_name = topic_arn.split(":")[-1]

    with mock_sqs():
        with mock_sns():

            cli = boto3.client("sns")
            cli.create_topic(Name=topic_name)

            sqs = boto3.client("sqs")
            sqs.create_queue(QueueName="TestQueue")

            cli.subscribe(
                TopicArn=topic_arn,
                Protocol="sqs",
                Endpoint=f"arn:aws:sqs:us-east-1:{'1'*12}:TestQueue",
            )

            yield sqs


@pytest.fixture
def trace_enabled():
    os.environ["_X_AMZN_TRACE_ID"] = TRACE_ID
    yield
    del os.environ["_X_AMZN_TRACE_ID"]


DATASET_GROUP_NAME = "DATASET_GROUP_NAME"


@pytest.fixture
def context():
    ctx = namedtuple("Context", ["invoked_function_arn"])
    return ctx(f"arn:aws:lambda:us-east-1:{'1' * 12}:function:my-function:1")


def test_sns_notification(context, sqs_mock):
    """Test without traces"""
    lambda_handler(
        {
            "datasetGroup": DATASET_GROUP_NAME,
            "statesError": {
                "Cause": '{"errorMessage": "ERROR_MESSAGE"}',
            },
        },
        context,
    )

    url = sqs_mock.get_queue_url(QueueName="TestQueue")["QueueUrl"]
    msg = json.loads(
        json.loads(
            sqs_mock.receive_message(QueueUrl=url, MaxNumberOfMessages=1,)["Messages"][
                0
            ]["Body"]
        )["Message"]
    )

    error_default = (
        f"The personalization workflow for {DATASET_GROUP_NAME} completed with errors"
    )
    error_json = {
        "datasetGroup": DATASET_GROUP_NAME,
        "status": "UPDATE FAILED",
        "summary": f"The personalization workflow for {DATASET_GROUP_NAME} completed with errors",
        "description": f"There was an error running the personalization job for dataset group {DATASET_GROUP_NAME}\n\nMessage: ERROR_MESSAGE\n\n",
    }

    assert msg["default"] == error_default
    assert msg["sms"] == error_default
    assert json.loads(msg["sqs"]) == error_json


def test_sns_notification_trace(sqs_mock, trace_enabled, context):
    """Test with traces"""
    lambda_handler(
        {
            "datasetGroup": DATASET_GROUP_NAME,
            "statesError": {
                "Cause": '{"errorMessage": "ERROR_MESSAGE"}',
            },
        },
        context,
    )

    url = sqs_mock.get_queue_url(QueueName="TestQueue")["QueueUrl"]
    msg = json.loads(
        json.loads(
            sqs_mock.receive_message(QueueUrl=url, MaxNumberOfMessages=1,)["Messages"][
                0
            ]["Body"]
        )["Message"]
    )

    error_default = (
        f"The personalization workflow for {DATASET_GROUP_NAME} completed with errors"
    )
    error_json = {
        "datasetGroup": f"{DATASET_GROUP_NAME}",
        "status": "UPDATE FAILED",
        "summary": f"The personalization workflow for {DATASET_GROUP_NAME} completed with errors",
        "description": f"There was an error running the personalization job for dataset group {DATASET_GROUP_NAME}\n\nMessage: ERROR_MESSAGE\n\nTraces: https://console.aws.amazon.com/xray/home?region=us-east-1#/traces/1-57f5498f-d91047849216d0f2ea3b6442",
    }

    assert msg["default"] == error_default
    assert msg["sms"] == error_default
    assert json.loads(msg["sqs"]) == error_json
