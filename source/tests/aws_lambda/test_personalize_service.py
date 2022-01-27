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
import binascii
import json
import os
from datetime import datetime

import boto3
import pytest
from dateutil import tz
from dateutil.tz import tzlocal
from moto import mock_s3, mock_sts

from aws_lambda.shared.personalize_service import (
    S3,
    Personalize,
    Configuration,
    get_duplicates,
)
from shared.exceptions import ResourceNeedsUpdate, ResourceFailed
from shared.personalize.service_model import ServiceModel
from shared.resource import Campaign


@pytest.fixture
def config_empty(tmp_path):
    f = tmp_path / "config.json"
    f.write_text("{}")
    yield f


@pytest.fixture
def mocked_s3():
    with mock_s3():
        cli = boto3.client("s3")
        cli.create_bucket(Bucket="test")
        cli.put_object(Bucket="test", Key="test.csv", Body="some_body")
        cli.put_object(Bucket="test", Key="sub/test.csv", Body="some_body")
        cli.put_object(Bucket="test", Key="sub/sub/test.csv", Body="some_body")
        cli.put_object(Bucket="test", Key="sub1/", Body="")
        yield boto3.resource("s3")


@pytest.fixture
def describe_solution_version_response():
    return {
        "solutionVersion": {
            "solutionVersionArn": f'arn:aws:personalize:us-east-1:{"1" * 12}:solution/personalize-integration-test-ranking/dfcd6f6e',
            "solutionArn": f'arn:aws:personalize:us-east-1:{"1" * 12}:solution/personalize-integration-test-ranking',
            "performHPO": False,
            "performAutoML": False,
            "recipeArn": "arn:aws:personalize:::recipe/aws-user-personalization",
            "datasetGroupArn": f'arn:aws:personalize:us-east-1:{"1" * 12}:dataset-group/personalize-integration-test',
            "solutionConfig": {},
            "trainingHours": 1.546,
            "trainingMode": "FULL",
            "status": "ACTIVE",
            "creationDateTime": datetime(
                2021, 9, 2, 14, 54, 56, 406000, tzinfo=tzlocal()
            ),
            "lastUpdatedDateTime": datetime(
                2021, 9, 2, 15, 16, 23, 424000, tzinfo=tzlocal()
            ),
        },
        "ResponseMetadata": {},
    }


@pytest.mark.parametrize(
    "url,bucket,key",
    (
        ["s3://test/test.csv", "test", "test.csv"],
        ["s3://test/sub/test.csv", "test", "sub/test.csv"],
        ["s3://test/sub/sub/test.csv", "test", "sub/sub/test.csv"],
    ),
)
def test_s3_urlparse(mocked_s3, url, bucket, key):
    s3 = S3(url)
    assert s3.url == url
    assert s3.bucket == bucket
    assert s3.key == key


def test_s3_exists_csv(mocked_s3):
    s3 = S3("s3://test/sub/test.csv")
    s3.cli = mocked_s3

    assert s3.exists
    assert s3.last_modified


def test_s3_exists_path(mocked_s3):
    s3 = S3("s3://test/sub")
    s3.cli = mocked_s3

    assert s3.exists
    assert s3.last_modified


def test_no_such_key_csv(mocked_s3):
    s3 = S3("s3://test/DOES_NOT_EXIST.csv")
    s3.cli = mocked_s3

    assert not s3.exists
    assert not s3.last_modified


def test_no_such_key_path(mocked_s3):
    s3 = S3("s3://test/DOES_NOT_EXIST")
    s3.cli = mocked_s3

    assert not s3.exists
    assert not s3.last_modified


@mock_sts
def test_service_model(personalize_stubber):
    cli = Personalize()

    # set up a service response for the depth-first listing structure expected
    dataset_group_name_1 = "dsg1"
    dataset_group_name_2 = "dsg2"
    solution_name_1 = "sol1"
    solution_name_2 = "sol2"
    filter_name_1 = "filter1"
    filter_name_2 = "filter2"
    campaign_name_1 = "campaign1"
    campaign_name_2 = "campaign2"
    dataset_group_arn_1 = (
        f"arn:aws:personalize:us-east-1:{'1' * 12}:dataset-group/{dataset_group_name_1}"
    )
    dataset_group_arn_2 = (
        f"arn:aws:personalize:us-east-1:{'1' * 12}:dataset-group/{dataset_group_name_2}"
    )
    dataset_arn_1 = f"arn:aws:personalize:us-east-1:{'1' * 12}:dataset/{dataset_group_name_1}/INTERACTIONS"
    dataset_arn_2 = f"arn:aws:personalize:us-east-1:{'1' * 12}:dataset/{dataset_group_name_2}/INTERACTIONS"
    solution_arn_1 = (
        f"arn:aws:personalize:us-east-1:{'1' * 12}:solution/{solution_name_1}"
    )
    solution_arn_2 = (
        f"arn:aws:personalize:us-east-1:{'1' * 12}:solution/{solution_name_2}"
    )
    filter_arn_1 = f"arn:aws:personalize:us-east-1:{'1' * 12}:filter/{filter_name_1}"
    filter_arn_2 = f"arn:aws:personalize:us-east-1:{'1' * 12}:filter/{filter_name_2}"
    campaign_arn_1 = (
        f"arn:aws:personalize:us-east-1:{'1' * 12}:campaign/{campaign_name_1}"
    )
    campaign_arn_2 = (
        f"arn:aws:personalize:us-east-1:{'1' * 12}:campaign/{campaign_name_2}"
    )

    # all dataset groups
    personalize_stubber.add_response(
        method="list_dataset_groups",
        service_response={
            "datasetGroups": [
                {"datasetGroupArn": dataset_group_arn_1},
                {"datasetGroupArn": dataset_group_arn_2},
            ]
        },
    )

    # first dataset group
    personalize_stubber.add_response(
        method="list_datasets",
        expected_params={"datasetGroupArn": dataset_group_arn_1},
        service_response={"datasets": [{"datasetArn": dataset_arn_1}]},
    )
    personalize_stubber.add_response(
        method="list_dataset_import_jobs",
        expected_params={"datasetArn": dataset_arn_1},
        service_response={"datasetImportJobs": []},
    )
    personalize_stubber.add_response(
        method="list_filters",
        expected_params={"datasetGroupArn": dataset_group_arn_1},
        service_response={"Filters": [{"filterArn": filter_arn_1}]},
    )
    personalize_stubber.add_response(
        method="list_solutions",
        expected_params={"datasetGroupArn": dataset_group_arn_1},
        service_response={"solutions": [{"solutionArn": solution_arn_1}]},
    )
    personalize_stubber.add_response(
        method="list_campaigns",
        expected_params={"solutionArn": solution_arn_1},
        service_response={"campaigns": [{"campaignArn": campaign_arn_1}]},
    )
    personalize_stubber.add_response(
        method="list_solution_versions",
        expected_params={"solutionArn": solution_arn_1},
        service_response={"solutionVersions": []},
    )
    personalize_stubber.add_response(
        method="list_recommenders",
        expected_params={"datasetGroupArn": dataset_group_arn_1},
        service_response={"recommenders": []},
    )
    personalize_stubber.add_response(
        method="list_event_trackers",
        expected_params={"datasetGroupArn": dataset_group_arn_1},
        service_response={"eventTrackers": []},
    )

    # second dataset group
    personalize_stubber.add_response(
        method="list_datasets",
        expected_params={"datasetGroupArn": dataset_group_arn_2},
        service_response={"datasets": [{"datasetArn": dataset_arn_2}]},
    )
    personalize_stubber.add_response(
        method="list_dataset_import_jobs",
        expected_params={"datasetArn": dataset_arn_2},
        service_response={"datasetImportJobs": []},
    )
    personalize_stubber.add_response(
        method="list_filters",
        expected_params={"datasetGroupArn": dataset_group_arn_2},
        service_response={"Filters": [{"filterArn": filter_arn_2}]},
    )
    personalize_stubber.add_response(
        method="list_solutions",
        expected_params={"datasetGroupArn": dataset_group_arn_2},
        service_response={"solutions": [{"solutionArn": solution_arn_2}]},
    )
    personalize_stubber.add_response(
        method="list_campaigns",
        expected_params={"solutionArn": solution_arn_2},
        service_response={"campaigns": [{"campaignArn": campaign_arn_2}]},
    )
    personalize_stubber.add_response(
        method="list_solution_versions",
        expected_params={"solutionArn": solution_arn_2},
        service_response={"solutionVersions": []},
    )
    personalize_stubber.add_response(
        method="list_recommenders",
        expected_params={"datasetGroupArn": dataset_group_arn_2},
        service_response={"recommenders": []},
    )
    personalize_stubber.add_response(
        method="list_event_trackers",
        expected_params={"datasetGroupArn": dataset_group_arn_2},
        service_response={"eventTrackers": []},
    )

    sm = ServiceModel(cli)

    assert sm.owned_by(filter_arn_1, dataset_group_arn_1)
    assert sm.owned_by(campaign_arn_1, dataset_group_name_1)
    assert sm.owned_by(filter_arn_2, dataset_group_arn_2)
    assert sm.owned_by(campaign_arn_2, dataset_group_name_2)
    for arn in [
        dataset_group_arn_1,
        dataset_group_arn_2,
        campaign_arn_1,
        campaign_arn_2,
        filter_arn_1,
        filter_arn_2,
        solution_arn_1,
        solution_arn_2,
    ]:
        assert not sm.available(arn)


@mock_sts
def test_configuration_valid(configuration_path):
    cfg = Configuration()
    cfg.load(configuration_path)
    validates = cfg.validate()
    assert validates


@mock_sts
def test_configuration_empty(config_empty):
    cfg = Configuration()
    cfg.load(config_empty)
    validates = cfg.validate()
    assert not validates


def test_get_duplicates_str():
    assert get_duplicates("hello") == []


def test_get_duplicates_list():
    assert get_duplicates([1, 2, 3]) == []


def test_get_duplicates_list_dup():
    assert get_duplicates([1, 1, 1, 2]) == [1]


def test_personalize_service_check_solution():
    personalize = Personalize()
    with pytest.raises(ResourceFailed):
        personalize._check_solution(
            "arn:aws:personalize:us-east-1:aaaaaaaaaaaa:solution/unit_test_solution_1/e970b8a3",
            "arn:aws:personalize:us-east-1:aaaaaaaaaaaa:solution/unit_test_solution_2/b944e180",
        )


def test_describe_with_update(mocker):
    personalize = Personalize()

    arn = "arn:aws:personalize:us-east-1:awsaccountid:solution/unit_test_solution_1/aaaaaaaa"

    describe_mock = mocker.MagicMock()
    describe_mock.return_value = {
        "campaign": {
            "solutionVersionArn": arn,
            "campaignArn": Campaign().arn("campaign_name"),
        }
    }
    personalize.describe_default = describe_mock

    assert (
        personalize.describe_with_update(resource=Campaign(), solutionVersionArn=arn)
        == describe_mock.return_value
    )

    with pytest.raises(ResourceNeedsUpdate):
        personalize.describe_with_update(
            resource=Campaign(), solutionVersionArn=arn.replace("aaaaaaaa", "bbbbbbbb")
        )


@pytest.mark.parametrize(
    "old_job,new_job,expected",
    [
        ({"status": "ACTIVE"}, None, True),
        ({"status": "CREATE PENDING"}, None, True),
        ({"status": "FAILED"}, None, False),
    ],
)
def test_is_current(old_job, new_job, expected):
    cli = Personalize()

    if not new_job:
        new_job = old_job

    assert cli.is_current(old_job, new_job) is expected


@mock_sts
def test_new_resource_solution_version(personalize_stubber):
    """describing a solution version with a maxAge and an ARN should resolve"""
    cli = Personalize()

    solution_name = "solution_name"
    solution_arn = f"arn:aws:personalize:us-east-1:{'1' * 12}:solution/{solution_name}"

    def solution_version_arn():
        return f"arn:aws:personalize:us-east-1:{'1' * 12}:solution/{solution_name}/{binascii.b2a_hex(os.urandom(4)).decode('utf-8')}"

    sv_arn_old = solution_version_arn()
    sv_arn_new = solution_version_arn()

    personalize_stubber.add_response(
        method="list_solution_versions",
        expected_params={"solutionArn": solution_arn},
        service_response={
            "solutionVersions": [
                {
                    "creationDateTime": datetime(
                        1999, 1, 1, 0, 0, 0, tzinfo=tz.tzlocal()
                    ),
                    "lastUpdatedDateTime": datetime(
                        2000, 1, 1, 0, 0, 0, tzinfo=tz.tzlocal()
                    ),
                    "status": "ACTIVE",
                    "solutionVersionArn": sv_arn_old,
                },
                {
                    "creationDateTime": datetime(
                        1999, 1, 2, 0, 0, 0, tzinfo=tz.tzlocal()
                    ),
                    "lastUpdatedDateTime": datetime(
                        2000, 1, 2, 0, 0, 0, tzinfo=tz.tzlocal()
                    ),
                    "status": "ACTIVE",
                    "solutionVersionArn": sv_arn_new,
                },
            ]
        },
    )
    personalize_stubber.add_response(
        method="describe_solution_version",
        service_response={
            "solutionVersion": {
                "solutionVersionArn": sv_arn_new,
                "solutionArn": solution_arn,
            }
        },
    )

    result = cli.describe_solution_version(
        trainingMode="FULL",
        solutionArn=solution_arn,
        maxAge=1,
        solutionVersionArn=sv_arn_new,
    )
    assert result["solutionVersion"]["solutionVersionArn"] == sv_arn_new


@mock_sts
def test_new_resource_solution_version(personalize_stubber):
    """describing a solution version with a maxAge and no ARN should result in not found"""
    cli = Personalize()

    solution_name = "solution_name"
    solution_arn = f"arn:aws:personalize:us-east-1:{'1' * 12}:solution/{solution_name}"

    def solution_version_arn():
        return f"arn:aws:personalize:us-east-1:{'1' * 12}:solution/{solution_name}/{binascii.b2a_hex(os.urandom(4)).decode('utf-8')}"

    sv_arn_old = solution_version_arn()
    sv_arn_new = solution_version_arn()

    personalize_stubber.add_response(
        method="list_solution_versions",
        expected_params={"solutionArn": solution_arn},
        service_response={
            "solutionVersions": [
                {
                    "creationDateTime": datetime(
                        1999, 1, 1, 0, 0, 0, tzinfo=tz.tzlocal()
                    ),
                    "lastUpdatedDateTime": datetime(
                        2000, 1, 1, 0, 0, 0, tzinfo=tz.tzlocal()
                    ),
                    "status": "ACTIVE",
                    "solutionVersionArn": sv_arn_old,
                },
                {
                    "creationDateTime": datetime(
                        1999, 1, 2, 0, 0, 0, tzinfo=tz.tzlocal()
                    ),
                    "lastUpdatedDateTime": datetime(
                        2000, 1, 2, 0, 0, 0, tzinfo=tz.tzlocal()
                    ),
                    "status": "ACTIVE",
                    "solutionVersionArn": sv_arn_new,
                },
            ]
        },
    )
    personalize_stubber.add_response(
        method="describe_solution_version",
        service_response={
            "solutionVersion": {
                "solutionVersionArn": sv_arn_new,
                "solutionArn": solution_arn,
            }
        },
    )

    with pytest.raises(cli.exceptions.ResourceNotFoundException):
        cli.describe_solution_version(
            trainingMode="FULL",
            solutionArn=solution_arn,
            maxAge=1,
        )


def test_record_offline_metrics(
    personalize_stubber, capsys, describe_solution_version_response
):
    personalize = Personalize()
    personalize_stubber.add_response(
        method="get_solution_metrics",
        service_response={
            "solutionVersionArn": f'arn:aws:personalize:us-east-1:{"1"*12}:solution/personalize-integration-test-ranking/dfcd6f6e',
            "metrics": {
                "coverage": 0.3235,
                "mean_reciprocal_rank_at_25": 0.3274,
                "normalized_discounted_cumulative_gain_at_10": 0.332,
                "normalized_discounted_cumulative_gain_at_25": 0.4746,
                "normalized_discounted_cumulative_gain_at_5": 0.2338,
                "precision_at_10": 0.15,
                "precision_at_25": 0.13,
                "precision_at_5": 0.15,
            },
            "ResponseMetadata": {},
        },
    )
    personalize._record_offline_metrics(
        solution_version=describe_solution_version_response
    )

    log = capsys.readouterr().out.strip()
    metrics = json.loads(log)

    assert metrics["service"] == "SolutionMetrics"
    assert metrics["solutionArn"]
    assert metrics["coverage"]
    assert metrics["mean_reciprocal_rank_at_25"]
    assert metrics["normalized_discounted_cumulative_gain_at_5"]
    assert metrics["normalized_discounted_cumulative_gain_at_10"]
    assert metrics["normalized_discounted_cumulative_gain_at_25"]
    assert metrics["precision_at_5"]
    assert metrics["precision_at_10"]
    assert metrics["precision_at_25"]


def test_solution_version_update_validation():
    cfg = Configuration()
    cfg.config_dict = {
        "solutions": [
            {
                "serviceConfig": {
                    "name": "valid",
                    "recipeArn": "arn:aws:personalize:::recipe/aws-user-personalization",
                },
                "workflowConfig": {
                    "schedules": {
                        "full": "cron(0 0 ? * 1 *)",
                        "update": "cron(0 * * * ? *)",
                    }
                },
            },
            {
                "serviceConfig": {
                    "name": "valid",
                    "recipeArn": "arn:aws:personalize:::recipe/aws-sims",
                },
                "workflowConfig": {
                    "schedules": {
                        "full": "cron(0 0 ? * 1 *)",
                    }
                },
            },
            {
                "serviceConfig": {
                    "name": "valid",
                    "recipeArn": "arn:aws:personalize:::recipe/aws-hrnn-coldstart",
                },
                "workflowConfig": {
                    "schedules": {
                        "full": "cron(0 0 ? * 1 *)",
                        "update": "cron(0 * * * ? *)",
                    }
                },
            },
            {
                "serviceConfig": {
                    "name": "invalid",
                    "recipeArn": "arn:aws:personalize:::recipe/aws-sims",
                },
                "workflowConfig": {
                    "schedules": {
                        "full": "cron(0 0 ? * 1 *)",
                        "update": "cron(0 * * * ? *)",
                    }
                },
            },
        ]
    }
    cfg._validate_solution_update()
    assert len(cfg._configuration_errors) == 1
    assert cfg._configuration_errors[0].startswith("solution invalid does not support")
