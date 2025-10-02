# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta

import pytest
from aws_lambda.create_dataset.handler import CONFIG, RESOURCE, lambda_handler
from botocore.exceptions import ParamValidationError
from dateutil.tz import tzlocal
from moto import mock_sts
from shared.exceptions import ResourcePending
from shared.resource import Dataset, DatasetGroup

dataset_group_name = "mockDatasetGroup"
dataset_name = "mockDataset"


def test_create_dataset_handler(validate_handler_config):
    validate_handler_config(RESOURCE, CONFIG)
    with pytest.raises(ValueError):
        lambda_handler({}, None)


@mock_sts
def test_dataset_tags(personalize_stubber, notifier_stubber):
    dataset_arn = Dataset().arn(dataset_name)
    dataset_group_arn = DatasetGroup().arn(dataset_group_name)

    personalize_stubber.add_response(
        method="list_datasets",
        expected_params={"datasetGroupArn": dataset_group_arn},
        service_response={"datasets": []},
    )

    personalize_stubber.add_response(
        method="create_dataset",
        expected_params={
            "name": dataset_name,
            "schemaArn": "schemaArn",
            "datasetGroupArn": dataset_group_arn,
            "datasetType": "INTERACTIONS",
            "tags": [
                {"tagKey": "dataset-1", "tagValue": "dataset-key-1"},
            ],
        },
        service_response={"datasetArn": dataset_arn},
    )

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {
                    "name": dataset_name,
                    "schemaArn": "schemaArn",
                    "datasetGroupArn": dataset_group_arn,
                    "datasetType": "INTERACTIONS",
                    "tags": [{"tagKey": "dataset-1", "tagValue": "dataset-key-1"}],
                }
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "CREATING"


@mock_sts
def test_bad_dataset_tags(personalize_stubber):
    dataset_arn = Dataset().arn(dataset_name)
    dataset_group_arn = DatasetGroup().arn(dataset_group_name)

    personalize_stubber.add_response(
        method="list_datasets",
        expected_params={"datasetGroupArn": dataset_group_arn},
        service_response={"datasets": []},
    )

    personalize_stubber.add_response(
        method="create_dataset",
        expected_params={
            "name": dataset_name,
            "schemaArn": "schemaArn",
            "datasetGroupArn": dataset_group_arn,
            "datasetType": "INTERACTIONS",
            "tags": "bad data",
        },
        service_response={"datasetArn": dataset_arn},
    )

    try:
        lambda_handler(
            {
                "serviceConfig": {
                    "name": dataset_name,
                    "schemaArn": "schemaArn",
                    "datasetGroupArn": dataset_group_arn,
                    "datasetType": "INTERACTIONS",
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
