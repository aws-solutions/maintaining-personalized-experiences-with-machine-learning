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
from os import environ

import boto3
import pytest
from moto import mock_s3, mock_stepfunctions, mock_sns, mock_sts

from aws_lambda.s3_event.handler import lambda_handler
from aws_solutions.core.helpers import _helpers_service_clients


@pytest.fixture
def s3_event():
    return {
        "Records": [
            {
                "eventVersion": "2.2",
                "eventSource": "aws:s3",
                "awsRegion": "us-west-2",
                "eventTime": "The time, in ISO-8601 format, for example, 1970-01-01T00:00:00.000Z, when Amazon S3 finished processing the request",
                "eventName": "event-type",
                "userIdentity": {
                    "principalId": "Amazon-customer-ID-of-the-user-who-caused-the-event"
                },
                "requestParameters": {
                    "sourceIPAddress": "ip-address-where-request-came-from"
                },
                "responseElements": {
                    "x-amz-request-id": "Amazon S3 generated request ID",
                    "x-amz-id-2": "Amazon S3 host that processed the request",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "ID found in the bucket notification configuration",
                    "bucket": {
                        "name": "bucket-name",
                        "ownerIdentity": {
                            "principalId": "Amazon-customer-ID-of-the-bucket-owner"
                        },
                        "arn": "bucket-ARN",
                    },
                    "object": {
                        "key": "train/object-key.json",
                        "size": "object-size",
                        "eTag": "object eTag",
                        "versionId": "object version if bucket is versioning-enabled, otherwise null",
                        "sequencer": "a string representation of a hexadecimal value used to determine event sequence, only used with PUTs and DELETEs",
                    },
                },
                "glacierEventData": {
                    "restoreEventData": {
                        "lifecycleRestorationExpiryTime": "The time, in ISO-8601 format, for example, 1970-01-01T00:00:00.000Z, of Restore Expiry",
                        "lifecycleRestoreStorageClass": "Source storage class for restore",
                    }
                },
            }
        ]
    }


@pytest.fixture
def simple_definition():
    definition = {
        "StartAt": "FirstState",
        "States": {
            "Type": "Task",
            "Resource": f"arn:aws:lambda:us-east-1:{'1'*12}:function:FUNCTION_NAME",
            "End": True,
        },
    }
    return json.dumps(definition)


@pytest.fixture
def stepfunctions_mocked(simple_definition):
    with mock_stepfunctions():
        client = boto3.client("stepfunctions")
        client.create_state_machine(
            name="personalize-workflow",
            definition=simple_definition,
            roleArn=f"arn:aws:iam::{'1' * 12}:role/sf_role",
        )
        _helpers_service_clients["stepfunctions"] = client
        yield client


@pytest.fixture
def s3_mocked(s3_event, configuration_path):
    with mock_s3():
        client = boto3.client("s3")
        client.create_bucket(Bucket="bucket-name")
        client.put_object(
            Bucket="bucket-name",
            Key="train/object-key.json",
            Body=configuration_path.read_text(),
        )
        _helpers_service_clients["s3"] = client
        yield client


@pytest.fixture
def sns_mocked():
    with mock_sns():
        client = boto3.client("sns")
        client.create_topic(
            Name="some-personalize-notification-topic",
        )
        _helpers_service_clients["sns"] = client
        yield client


@mock_sts
def test_s3_event_handler(s3_event, sns_mocked, s3_mocked, stepfunctions_mocked):
    lambda_handler(s3_event, None)

    # ensure that execution has started
    executions = stepfunctions_mocked.list_executions(
        stateMachineArn=environ.get("STATE_MACHINE_ARN"),
    )
    assert len(executions["executions"]) == 1
    assert executions["executions"][0]["status"] == "RUNNING"


@mock_sts
def test_s3_event_handler_working(
    s3_event, sns_mocked, s3_mocked, stepfunctions_mocked
):
    s3_mocked.put_object(
        Bucket="bucket-name",
        Key="train/object-key.json",
        Body=json.dumps({"datasetGroup": {"serviceConfig": {"name": "testDsg"}}}),
    )
    lambda_handler(s3_event, None)

    # ensure no executions started
    executions = stepfunctions_mocked.list_executions(
        stateMachineArn=environ.get("STATE_MACHINE_ARN"),
    )
    assert len(executions["executions"]) == 1


@mock_sts
def test_s3_event_handler_bad_json(
    s3_event, sns_mocked, s3_mocked, stepfunctions_mocked
):
    s3_mocked.put_object(Bucket="bucket-name", Key="train/object-key.json", Body="{")
    lambda_handler(s3_event, None)

    # ensure no executions started
    executions = stepfunctions_mocked.list_executions(
        stateMachineArn=environ.get("STATE_MACHINE_ARN"),
    )
    assert len(executions["executions"]) == 0


@mock_sts
def test_s3_event_handler_bad_config(
    s3_event, sns_mocked, s3_mocked, stepfunctions_mocked
):
    s3_mocked.put_object(
        Bucket="bucket-name",
        Key="train/object-key.json",
        Body='{"this": "is not configuration data"}',
    )
    lambda_handler(s3_event, None)

    # ensure no executions started
    executions = stepfunctions_mocked.list_executions(
        stateMachineArn=environ.get("STATE_MACHINE_ARN"),
    )
    assert len(executions["executions"]) == 0
