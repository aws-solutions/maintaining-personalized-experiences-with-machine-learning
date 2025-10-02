# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

import pytest
from aws_lambda.create_solution_version.handler import (
    CONFIG,
    RESOURCE,
    STATUS,
    lambda_handler,
)
from botocore.exceptions import ParamValidationError
from moto import mock_sts
from shared.exceptions import SolutionVersionPending
from shared.resource import Solution, SolutionVersion

solution_version_name = "abcdefghi"  # hash name of the solution_version


def test_create_solution_version_handler(validate_handler_config):
    validate_handler_config(RESOURCE, CONFIG, STATUS)
    with pytest.raises(ValueError):
        lambda_handler({}, None)


@mock_sts
def test_solutionv_tags(personalize_stubber, notifier_stubber):
    solutionv_arn = SolutionVersion().arn(solution_version_name)
    solution_arn = Solution().arn("solName")

    personalize_stubber.add_response(
        method="list_solution_versions",
        expected_params={"solutionArn": solution_arn},
        service_response={"solutionVersions": []},
    )

    personalize_stubber.add_response(
        method="create_solution_version",
        expected_params={
            "solutionArn": solution_arn,
            "trainingMode": "FULL",
            "tags": [
                {"tagKey": "solutionVersion-1", "tagValue": "solutionVersion-key-1"},
            ],
        },
        service_response={"solutionVersionArn": solutionv_arn},
    )

    with pytest.raises(SolutionVersionPending):
        lambda_handler(
            {
                "serviceConfig": {
                    "solutionArn": solution_arn,
                    "trainingMode": "FULL",
                    "tags": [{"tagKey": "solutionVersion-1", "tagValue": "solutionVersion-key-1"}],
                }
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "CREATING"


@mock_sts
def test_solutionv_bad_tags(personalize_stubber):
    solutionv_arn = SolutionVersion().arn(solution_version_name)
    solution_arn = Solution().arn("solName")

    personalize_stubber.add_response(
        method="list_solution_versions",
        expected_params={"solutionArn": solution_arn},
        service_response={"solutionVersions": []},
    )

    personalize_stubber.add_response(
        method="create_solution_version",
        expected_params={
            "solutionArn": solution_arn,
            "trainingMode": "FULL",
            "tags": "bad data",
        },
        service_response={"solutionVersionArn": solutionv_arn},
    )

    try:
        lambda_handler(
            {
                "serviceConfig": {
                    "solutionArn": solution_arn,
                    "trainingMode": "FULL",
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
