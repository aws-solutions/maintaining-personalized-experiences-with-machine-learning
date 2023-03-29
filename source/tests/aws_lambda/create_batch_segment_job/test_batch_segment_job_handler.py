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
from aws_lambda.create_batch_segment_job.handler import (
    CONFIG,
    RESOURCE,
    STATUS,
    lambda_handler,
)
from botocore.exceptions import ParamValidationError
from moto import mock_sts
from shared.exceptions import ResourcePending
from shared.resource import BatchSegmentJob, SolutionVersion

batch_segment_name = "mockBatchJob"
solution_version_name = "mockSolutionVersion"


def test_create_batch_segment_job_handler(validate_handler_config):
    validate_handler_config(RESOURCE, CONFIG, STATUS)
    with pytest.raises(ValueError):
        lambda_handler({}, None)


@mock_sts
def test_batch_segment_tags(monkeypatch, personalize_stubber, notifier_stubber):
    os.environ["ROLE_ARN"] = "roleArn"
    batch_segment_arn = BatchSegmentJob().arn(batch_segment_name)
    solution_version_arn = SolutionVersion().arn(solution_version_name)

    personalize_stubber.add_response(
        method="list_batch_segment_jobs",
        expected_params={
            "solutionVersionArn": solution_version_arn,
        },
        service_response={"batchSegmentJobs": []},
    )

    personalize_stubber.add_response(
        method="create_batch_segment_job",
        expected_params={
            "jobName": batch_segment_name,
            "solutionVersionArn": solution_version_arn,
            "jobInput": {"s3DataSource": {"path": "s3Path1", "kmsKeyArn": "kmsArn"}},
            "jobOutput": {"s3DataDestination": {"path": "s3Path2", "kmsKeyArn": "kmsArn"}},
            "roleArn": os.getenv("ROLE_ARN"),
            "tags": [
                {"tagKey": "batchSegment-1", "tagValue": "batchSegment-key-1"},
            ],
        },
        service_response={"batchSegmentJobArn": batch_segment_arn},
    )

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {
                    "jobName": batch_segment_name,
                    "jobInput": {"s3DataSource": {"path": "s3Path1", "kmsKeyArn": "kmsArn"}},
                    "jobOutput": {"s3DataDestination": {"path": "s3Path2", "kmsKeyArn": "kmsArn"}},
                    "tags": [{"tagKey": "batchSegment-1", "tagValue": "batchSegment-key-1"}],
                    "solutionVersionArn": solution_version_arn,
                }
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "CREATING"

    del os.environ["ROLE_ARN"]


@mock_sts
def test_bad_batch_segment_tags(personalize_stubber):
    os.environ["ROLE_ARN"] = "roleArn"
    batch_segment_arn = BatchSegmentJob().arn(batch_segment_name)
    solution_version_arn = SolutionVersion().arn(solution_version_name)

    personalize_stubber.add_response(
        method="list_batch_segment_jobs",
        expected_params={
            "solutionVersionArn": solution_version_arn,
        },
        service_response={"batchSegmentJobs": []},
    )

    personalize_stubber.add_response(
        method="create_batch_segment_job",
        expected_params={
            "jobName": batch_segment_name,
            "solutionVersionArn": solution_version_arn,
            "jobInput": {"s3DataSource": {"path": "s3Path1", "kmsKeyArn": "kmsArn"}},
            "jobOutput": {"s3DataDestination": {"path": "s3Path2", "kmsKeyArn": "kmsArn"}},
            "roleArn": os.getenv("ROLE_ARN"),
            "tags": "bad data",
        },
        service_response={"batchSegmentJobArn": batch_segment_arn},
    )

    try:
        lambda_handler(
            {
                "serviceConfig": {
                    "jobName": batch_segment_name,
                    "jobInput": {"s3DataSource": {"path": "s3Path1", "kmsKeyArn": "kmsArn"}},
                    "jobOutput": {"s3DataDestination": {"path": "s3Path2", "kmsKeyArn": "kmsArn"}},
                    "tags": "bad data",
                    "solutionVersionArn": solution_version_arn,
                }
            },
            None,
        )
    except ParamValidationError as exp:
        assert (
            exp.kwargs["report"]
            == "Invalid type for parameter tags, value: bad data, type: <class 'str'>, valid types: <class 'list'>, <class 'tuple'>"
        )

    del os.environ["ROLE_ARN"]
