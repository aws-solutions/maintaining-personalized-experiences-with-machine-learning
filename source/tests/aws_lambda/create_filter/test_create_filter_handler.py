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

import pytest
from aws_lambda.create_filter.handler import CONFIG, RESOURCE, STATUS, lambda_handler
from botocore.exceptions import ParamValidationError
from moto import mock_sts
from shared.exceptions import ResourcePending
from shared.resource import DatasetGroup, Filter

filter_name = "mockFilter"


def test_create_filter(validate_handler_config):
    validate_handler_config(RESOURCE, CONFIG, STATUS)
    with pytest.raises(ValueError):
        lambda_handler({}, None)


@mock_sts
def test_filter_tags(personalize_stubber, notifier_stubber):
    filter_arn = Filter().arn(filter_name)
    dataset_group_arn = DatasetGroup().arn("mockDatasetGroup")

    personalize_stubber.add_client_error(
        method="describe_filter",
        service_error_code="ResourceNotFoundException",
        expected_params={"filterArn": filter_arn},
    )

    personalize_stubber.add_response(
        method="create_filter",
        expected_params={
            "name": filter_name,
            "datasetGroupArn": dataset_group_arn,
            "filterExpression": "SOME-EXPRESSION",
            "tags": [
                {"tagKey": "filter-1", "tagValue": "filter-key-1"},
            ],
        },
        service_response={"filterArn": filter_arn},
    )

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {
                    "name": filter_name,
                    "datasetGroupArn": dataset_group_arn,
                    "filterExpression": "SOME-EXPRESSION",
                    "tags": [{"tagKey": "filter-1", "tagValue": "filter-key-1"}],
                }
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "CREATING"


@mock_sts
def test_bad_filter_tags(personalize_stubber):
    filter_arn = Filter().arn(filter_name)
    dataset_group_arn = DatasetGroup().arn("mockDatasetGroup")

    personalize_stubber.add_client_error(
        method="describe_filter",
        service_error_code="ResourceNotFoundException",
        expected_params={"filterArn": filter_arn},
    )

    personalize_stubber.add_response(
        method="create_filter",
        expected_params={
            "name": filter_name,
            "datasetGroupArn": dataset_group_arn,
            "filterExpression": "SOME-EXPRESSION",
            "tags": "bad data",
        },
        service_response={"filterArn": filter_arn},
    )

    try:
        lambda_handler(
            {
                "serviceConfig": {
                    "name": filter_name,
                    "datasetGroupArn": dataset_group_arn,
                    "filterExpression": "SOME-EXPRESSION",
                    "tags": "bad data",
                }
            },
            None,
        )
    except ParamValidationError as exp:
        assert (
            exp.kwargs["report"]
            == "Invalid type for parameter tags, value: bad data, type: <class 'str'>, valid types: <class 'list'>, <class 'tuple'>"
        )
