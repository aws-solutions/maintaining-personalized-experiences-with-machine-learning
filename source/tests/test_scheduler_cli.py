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

import pytest
import json
import boto3
from moto import mock_cloudformation
import os
from unittest import mock

from aws_solutions.scheduler.common.scripts.scheduler_cli import (
    get_stack_output_value,
    get_stack_tag_value,
    get_stack_metadata_value,
    setup_cli_env,
    get_payload,
)


TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Metadata": {
        "aws:solutions:templatename": "maintaining-personalized-experiences-with-machine-learning.template",
        "aws:solutions:solution_version": "solution_version_value",
        "aws:solutions:solution_id": "solution_id_value",
    },
    "Resources": {
        "QueueResource": {
            "Type": "AWS::SQS::Queue",
            "Properties": {"QueueName": "my-queue"},
        }
    },
    "Outputs": {"QueueOutput": {"Description": "The Queue Name", "Value": "my-queue"}},
}


@pytest.fixture
def stack(mocker):
    global TEMPLATE
    with mock_cloudformation():
        cli = boto3.client("cloudformation")
        cli.create_stack(
            StackName="TestStack",
            TemplateBody=json.dumps(TEMPLATE),
            Tags=[
                {
                    "Key": "TestTag",
                    "Value": "TestValue",
                },
                {"Key": "SOLUTION_ID", "Value": "SOLUTION_ID_VALUE"},
                {"Key": "SOLUTION_VERSION", "Value": "SOLUTION_VERSION_VALUE"},
            ],
        )
        resource = boto3.resource("cloudformation").Stack("TestStack")
        resource.meta.client.get_template_summary = mocker.MagicMock(
            return_value=TEMPLATE | {"Metadata": json.dumps(TEMPLATE["Metadata"])}
        )
        yield resource


def test_get_stack_output_value(stack):
    assert get_stack_output_value(stack, "QueueOutput") == "my-queue"


def test_get_stack_output_value_not_present(stack):
    with pytest.raises(ValueError):
        get_stack_output_value(stack, "missing")


def test_get_stack_tag_value(stack):
    assert get_stack_tag_value(stack, "TestTag") == "TestValue"


def test_get_stack_tag_value_not_present(stack):
    with pytest.raises(ValueError):
        get_stack_tag_value(stack, "missing")


def test_get_stack_metadata(stack, mocker):
    assert get_stack_metadata_value(stack, "aws:solutions:solution_id") == "solution_id_value"
    assert get_stack_metadata_value(stack, "aws:solutions:solution_version") == "solution_version_value"


def test_get_stack_metadata_not_present(stack, mocker):
    with pytest.raises(ValueError):
        get_stack_metadata_value(stack, "missing")


def test_setup_cli_env(stack):
    with mock.patch.dict(os.environ, {}):
        setup_cli_env(stack, "eu-central-1")
        assert os.environ.get("AWS_REGION") == "eu-central-1"
        assert os.environ.get("SOLUTION_ID") == "solution_id_value"
        assert os.environ.get("SOLUTION_VERSION") == "solution_version_value"


def test_get_payload():
    payload = get_payload(
        dataset_group="dsg",
        import_schedule="cron(* * * * ? *)",
        update_schedule=[
            ("a", "cron(0 * * * ? *)"),
            ("b", "cron(1 * * * ? *)"),
        ],
        full_schedule=[("c", "cron(3 * * * ? *)"), ("d", "cron(4 * * * ? *)")],
    )

    assert payload == {
        "datasetGroupName": "dsg",
        "schedules": {
            "import": "cron(* * * * ? *)",
            "solutions": {
                "a": {"update": "cron(0 * * * ? *)"},
                "b": {"update": "cron(1 * * * ? *)"},
                "c": {"full": "cron(3 * * * ? *)"},
                "d": {"full": "cron(4 * * * ? *)"},
            },
        },
    }
