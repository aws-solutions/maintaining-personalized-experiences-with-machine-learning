# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

import pytest
from aws_lambda.create_event_tracker.handler import CONFIG, RESOURCE, STATUS, lambda_handler
from botocore.exceptions import ParamValidationError
from shared.exceptions import ResourcePending
from shared.resource import DatasetGroup, EventTracker

etracker_name = "mockEventTracker"


def test_create_event_tracker(validate_handler_config):
    validate_handler_config(RESOURCE, CONFIG, STATUS)
    with pytest.raises(ValueError):
        lambda_handler({}, None)


def test_event_tracker_tags(mocker, personalize_stubber, notifier_stubber):
    event_tracker_arn = EventTracker().arn(etracker_name)
    dataset_group_arn = DatasetGroup().arn("mockDatasetGroup")

    personalize_stubber.add_response(
        method="list_event_trackers",
        expected_params={
            "datasetGroupArn": dataset_group_arn,
        },
        service_response={"eventTrackers": []},
    )

    personalize_stubber.add_response(
        method="create_event_tracker",
        expected_params={
            "name": etracker_name,
            "datasetGroupArn": dataset_group_arn,
            "tags": [
                {"tagKey": "et-1", "tagValue": "et-key-1"},
            ],
        },
        service_response={"eventTrackerArn": event_tracker_arn},
    )

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {
                    "name": etracker_name,
                    "datasetGroupArn": dataset_group_arn,
                    "tags": [{"tagKey": "et-1", "tagValue": "et-key-1"}],
                }
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "CREATING"


def test_bad_event_tracker_tags(mocker, personalize_stubber):
    event_tracker_arn = EventTracker().arn(etracker_name)
    dataset_group_arn = DatasetGroup().arn("mockDatasetGroup")

    personalize_stubber.add_response(
        method="list_event_trackers",
        expected_params={
            "datasetGroupArn": dataset_group_arn,
        },
        service_response={"eventTrackers": []},
    )

    personalize_stubber.add_response(
        method="create_event_tracker",
        expected_params={
            "name": etracker_name,
            "datasetGroupArn": dataset_group_arn,
            "tags": "bad data",
        },
        service_response={"eventTrackerArn": event_tracker_arn},
    )

    try:
        lambda_handler(
            {
                "serviceConfig": {
                    "name": etracker_name,
                    "datasetGroupArn": dataset_group_arn,
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
