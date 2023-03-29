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
from aws_lambda.create_dataset_import_job.handler import (
    CONFIG,
    RESOURCE,
    STATUS,
    lambda_handler,
)
from botocore.exceptions import ParamValidationError
from moto import mock_sts
from shared.exceptions import ResourcePending
from shared.resource import Dataset, DatasetGroup, DatasetImportJob

dataset_name = "mockDataset"
dataset_arn = Dataset().arn(dataset_name)
dataset_import_arn = DatasetImportJob().arn("mockDatasetImport")
dataset_group_arn = DatasetGroup().arn("mockDatasetGroup")


def test_create_dataset_import_job_handler(validate_handler_config):
    validate_handler_config(RESOURCE, CONFIG, STATUS)
    with pytest.raises(ValueError):
        lambda_handler({}, None)


@mock_sts
def test_data_import_tags(mocker, personalize_stubber, notifier_stubber):
    os.environ["ROLE_ARN"] = "roleArn"
    dataset_arn = Dataset().arn(dataset_name)
    dataset_import_arn = DatasetImportJob().arn("mockDatasetImport")

    personalize_stubber.add_response(
        method="list_dataset_import_jobs",
        expected_params={"datasetArn": dataset_arn},
        service_response={"datasetImportJobs": []},
    )

    personalize_stubber.add_response(
        method="create_dataset_import_job",
        expected_params={
            "jobName": dataset_name,
            "datasetArn": dataset_arn,
            "dataSource": {"dataLocation": "s3://path/to/file"},
            "roleArn": os.getenv("ROLE_ARN"),
            "tags": [
                {"tagKey": "datasetImport-1", "tagValue": "datasetImport-key-1"},
            ],
            "importMode": "FULL",
            "publishAttributionMetricsToS3": True,
        },
        service_response={"datasetImportJobArn": dataset_import_arn},
    )

    mocker.patch("shared.s3.S3.exists", True)

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {
                    "jobName": dataset_name,
                    "datasetArn": dataset_arn,
                    "dataSource": {"dataLocation": "s3://path/to/file"},
                    "roleArn": os.getenv("ROLE_ARN"),
                    "tags": [
                        {"tagKey": "datasetImport-1", "tagValue": "datasetImport-key-1"},
                    ],
                    "importMode": "FULL",
                    "publishAttributionMetricsToS3": True,
                }
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "CREATING"

    del os.environ["ROLE_ARN"]


@mock_sts
def test_bad_data_import_tags(mocker, personalize_stubber):
    dataset_arn = Dataset().arn(dataset_name)
    dataset_import_arn = DatasetImportJob().arn("mockDatasetImport")

    os.environ["ROLE_ARN"] = "roleArn"

    personalize_stubber.add_response(
        method="list_dataset_import_jobs",
        expected_params={"datasetArn": dataset_arn},
        service_response={"datasetImportJobs": []},
    )

    personalize_stubber.add_response(
        method="create_dataset_import_job",
        expected_params={
            "jobName": dataset_name,
            "datasetArn": dataset_arn,
            "dataSource": {"dataLocation": "s3://path/to/file"},
            "roleArn": os.getenv("ROLE_ARN"),
            "tags": "bad data",
            "importMode": "FULL",
            "publishAttributionMetricsToS3": True,
        },
        service_response={"datasetImportJobArn": dataset_import_arn},
    )

    mocker.patch("shared.s3.S3.exists", True)

    try:
        lambda_handler(
            {
                "serviceConfig": {
                    "jobName": dataset_name,
                    "datasetArn": dataset_arn,
                    "dataSource": {"dataLocation": "s3://path/to/file"},
                    "roleArn": os.getenv("ROLE_ARN"),
                    "tags": "bad data",
                    "importMode": "FULL",
                    "publishAttributionMetricsToS3": True,
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
