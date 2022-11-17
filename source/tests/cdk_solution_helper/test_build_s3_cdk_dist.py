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
from pathlib import Path

import boto3
import botocore
import click
import pytest
from moto import mock_s3, mock_sts

from aws_solutions.cdk.scripts.build_s3_cdk_dist import (
    PathPath,
    BuildEnvironment,
    RegionalAssetPackager,
    GlobalAssetPackager,
    validate_version_code,
    BaseAssetPackager,
)

TEST_VERSION_CODE = "v1.0.0"


@pytest.fixture
def default_build_environment():
    build = BuildEnvironment(
        source_bucket_name="source_bucket",
        solution_name="solution-name",
        version_code=TEST_VERSION_CODE,
    )
    gap = GlobalAssetPackager(build)
    rap = RegionalAssetPackager(build)
    gap.check_bucket = lambda: True
    rap.check_bucket = lambda: True

    return gap, rap


def test_pathpath():
    pth = PathPath()
    test_path = pth("test")
    assert isinstance(test_path, Path)


def test_build_environment():
    build = BuildEnvironment(
        source_bucket_name="source_bucket",
        solution_name="solution-name",
        version_code=TEST_VERSION_CODE,
    )
    assert Path(build.template_dist_dir).stem == "global-s3-assets"
    assert Path(build.build_dir).stem == "build-s3-assets"
    assert Path(build.build_dist_dir).stem == "regional-s3-assets"
    assert Path(build.source_dir).stem == "source"
    assert Path(build.infrastructure_dir).parent.stem == "source"


def test_validate_version_code_valid():
    assert validate_version_code(None, None, TEST_VERSION_CODE) == TEST_VERSION_CODE


def test_validate_version_code_invalid():
    with pytest.raises(click.BadParameter):
        assert validate_version_code(None, None, "1.0.0")


@pytest.mark.parametrize(
    "packager_cls,expected_s3_path",
    [
        (GlobalAssetPackager, "s3://source_bucket/solution-name/v1.0.0"),
        (RegionalAssetPackager, "s3://source_bucket-us-east-1/solution-name/v1.0.0"),
    ],
)
def test_global_asset_packager(packager_cls, expected_s3_path):
    build = BuildEnvironment(
        source_bucket_name="source_bucket",
        solution_name="solution-name",
        version_code=TEST_VERSION_CODE,
    )
    packager = packager_cls(build)

    assert packager.s3_asset_path == expected_s3_path


def test_sync(mocker, default_build_environment):
    packager, _ = default_build_environment

    mock = mocker.MagicMock()
    type(mock.Popen.return_value.__enter__.return_value).stdout = mocker.PropertyMock(
        return_value=["sync stdout result"]
    )
    type(mock.Popen.return_value.__enter__.return_value).stderr = mocker.PropertyMock(
        return_value=["sync stderr result"]
    )
    type(mock.Popen.return_value.__enter__.return_value).returncode = mocker.PropertyMock(return_value=0)
    # fmt: off
    mocker.patch("aws_solutions.cdk.scripts.build_s3_cdk_dist.subprocess", mock)  # NOSONAR (python:S1192) - string for clarity
    # fmt: on
    packager.sync()


def test_sync_fail_no_awscli(mocker, default_build_environment):
    packager, _ = default_build_environment

    mock = mocker.MagicMock()
    mock.Popen.return_value.side_effect = FileNotFoundError()
    # fmt: off
    mocker.patch("aws_solutions.cdk.scripts.build_s3_cdk_dist.subprocess", mock)  # NOSONAR (python:S1192) - string for clarity
    # fmt: on

    with pytest.raises(click.ClickException):
        packager.sync()


def test_sync_fail_no_successful_awscli(mocker, default_build_environment):
    packager, _ = default_build_environment

    mock = mocker.MagicMock()
    # fmt: off
    mocker.patch("aws_solutions.cdk.scripts.build_s3_cdk_dist.subprocess", mock)  # NOSONAR (python:S1192) - string for clarity
    # fmt: on
    type(mock.Popen.return_value.__enter__.return_value).returncode = mocker.PropertyMock(return_value=1)

    with pytest.raises(click.ClickException):
        packager.sync()


@mock_s3
@mock_sts
def test_bucket_check_valid():
    s3 = boto3.client("s3", region_name="eu-central-1")
    s3.create_bucket(
        Bucket="MyBucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
    )

    packager = BaseAssetPackager()
    packager.s3_asset_path = "s3://MyBucket"
    assert packager.check_bucket()


@mock_s3
@mock_sts
def test_bucket_check_invalid():
    packager = BaseAssetPackager()
    packager.s3_asset_path = "s3://MyBucket"

    with pytest.raises(botocore.exceptions.ClientError):
        assert packager.check_bucket()
