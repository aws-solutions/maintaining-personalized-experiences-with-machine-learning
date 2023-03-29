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
from aws_lambda.create_recommender.handler import (
    CONFIG,
    RESOURCE,
    STATUS,
    lambda_handler,
)
from botocore.exceptions import ParamValidationError
from moto import mock_sts
from shared.exceptions import ResourcePending
from shared.resource import DatasetGroup, Recommender

recommender_name = "recommender-1"


def test_create_recommender_handler(validate_handler_config):
    validate_handler_config(RESOURCE, CONFIG, STATUS)
    with pytest.raises(ValueError):
        lambda_handler({}, None)


@mock_sts
def test_recommender_tags(personalize_stubber, notifier_stubber):
    recommender_arn = Recommender().arn(recommender_name)
    dataset_group_arn = DatasetGroup().arn("mockDatasetGroup")
    personalize_stubber.add_client_error(
        method="describe_recommender",
        service_error_code="ResourceNotFoundException",
        expected_params={"recommenderArn": recommender_arn},
    )

    personalize_stubber.add_response(
        method="create_recommender",
        expected_params={
            "name": recommender_name,
            "datasetGroupArn": dataset_group_arn,
            "recipeArn": "recipeArn",
            "tags": [
                {"tagKey": "recommender-1", "tagValue": "recommender-key-1"},
            ],
        },
        service_response={"recommenderArn": recommender_arn},
    )

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {
                    "name": recommender_name,
                    "datasetGroupArn": dataset_group_arn,
                    "recipeArn": "recipeArn",
                    "tags": [{"tagKey": "recommender-1", "tagValue": "recommender-key-1"}],
                }
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "CREATING"


@mock_sts
def test_bad_recommender_tags(personalize_stubber):
    recommender_arn = Recommender().arn(recommender_name)
    dataset_group_arn = DatasetGroup().arn("mockDatasetGroup")
    personalize_stubber.add_client_error(
        method="describe_recommender",
        service_error_code="ResourceNotFoundException",
        expected_params={"recommenderArn": recommender_arn},
    )

    personalize_stubber.add_response(
        method="create_recommender",
        expected_params={
            "name": recommender_name,
            "datasetGroupArn": dataset_group_arn,
            "recipeArn": "recipeArn",
            "tags": "bad data",
        },
        service_response={"recommenderArn": recommender_arn},
    )

    try:
        lambda_handler(
            {
                "serviceConfig": {
                    "name": recommender_name,
                    "datasetGroupArn": dataset_group_arn,
                    "recipeArn": "recipeArn",
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
