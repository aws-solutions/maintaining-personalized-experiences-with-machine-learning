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

from datetime import datetime, timedelta

import pytest
from aws_lambda.create_dataset_group.handler import (
    CONFIG,
    RESOURCE,
    STATUS,
    lambda_handler,
)
from botocore.exceptions import ParamValidationError
from dateutil.tz import tzlocal
from moto import mock_sts
from shared.exceptions import ResourcePending
from shared.personalize_service import Personalize
from shared.resource import DatasetGroup

dataset_group_name = "mockDatasetGroup"


def test_handler(validate_handler_config):
    validate_handler_config(RESOURCE, CONFIG, STATUS)
    with pytest.raises(ValueError):
        lambda_handler({}, None)


@mock_sts
def test_dsg_tags(personalize_stubber, notifier_stubber):
    """
    The typical workflow is to describe, then create, then raise ResourcePending
    """
    dataset_group_arn = DatasetGroup().arn(dataset_group_name)
    personalize_stubber.add_client_error(
        method="describe_dataset_group",
        service_error_code="ResourceNotFoundException",
        expected_params={"datasetGroupArn": dataset_group_arn},
    )
    personalize_stubber.add_response(
        method="create_dataset_group",
        expected_params={
            "name": dataset_group_name,
            "tags": [
                {"tagKey": "datasetGroup-1", "tagValue": "datasetGroup-key-1"},
            ],
        },
        service_response={"datasetGroupArn": dataset_group_arn},
    )

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {
                    "name": dataset_group_name,
                    "tags": [{"tagKey": "datasetGroup-1", "tagValue": "datasetGroup-key-1"}],
                }
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "CREATING"


@mock_sts
def test_dsg_bad_tags(personalize_stubber):
    """
    The typical workflow is to describe, then create, then raise ResourcePending
    """
    dataset_group_arn = DatasetGroup().arn(dataset_group_name)
    personalize_stubber.add_client_error(
        method="describe_dataset_group",
        service_error_code="ResourceNotFoundException",
        expected_params={"datasetGroupArn": dataset_group_arn},
    )
    personalize_stubber.add_response(
        method="create_dataset_group",
        expected_params={
            "name": dataset_group_name,
            "tags": "bad data",
        },
        service_response={"datasetGroupArn": dataset_group_arn},
    )

    try:
        lambda_handler(
            {
                "serviceConfig": {
                    "name": dataset_group_name,
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


@mock_sts
def test_dsg_list_tags(personalize_stubber, notifier_stubber):
    """
    The typical workflow is to describe, then create, then raise ResourcePending
    """
    dsg_name = "mockDatasetGroup"
    dataset_group_arn = DatasetGroup().arn(dataset_group_name)
    personalize_stubber.add_response(
        method="describe_dataset_group",
        service_response={
            "datasetGroup": {
                "name": dsg_name,
                "datasetGroupArn": dataset_group_arn,
                "status": "ACTIVE",
                "lastUpdatedDateTime": datetime.now(tzlocal()),
                "creationDateTime": datetime.now(tz=tzlocal()) - timedelta(seconds=100),
                "roleArn": "roleArn",
                "kmsKeyArn": "kmsArn",
            }
        },
        expected_params={"datasetGroupArn": dataset_group_arn},
    )

    personalize_stubber.add_response(
        method="list_tags_for_resource",
        expected_params={"resourceArn": dataset_group_arn},
        service_response={
            "tags": [
                {"tagKey": "datasetGroup-1", "tagValue": "datasetGroup-key-1"},
            ]
        },
    )

    lambda_handler(
        {
            "serviceConfig": {
                "name": dsg_name,
                "tags": [{"tagKey": "datasetGroup-1", "tagValue": "datasetGroup-key-1"}],
            }
        },
        None,
    )

    cli = Personalize()
    arn = DatasetGroup().arn(dsg_name)
    assert cli.list_tags_for_resource(resourceArn=arn) == {
        "tags": [{"tagKey": "datasetGroup-1", "tagValue": "datasetGroup-key-1"}]
    }
