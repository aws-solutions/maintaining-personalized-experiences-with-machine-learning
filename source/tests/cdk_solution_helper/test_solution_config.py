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

import botocore
import pytest

import aws_solutions.core


@pytest.fixture(scope="function", autouse=True)
def reset_botocore_config():
    """remove botocore configuration before test"""
    aws_solutions.core.config._botocore_config = None


@pytest.fixture(
    params=[
        "SO0100",
        "SO0100a",
        "SO0100A",
    ]
)
def solution_id_valid(request):
    solution_id = request.param
    os.environ["SOLUTION_ID"] = solution_id
    yield solution_id
    del os.environ["SOLUTION_ID"]


@pytest.fixture(params=["S0100", "abc", "SO0x3ab"])
def solution_id_invalid(request):
    solution_id = request.param
    os.environ["SOLUTION_ID"] = solution_id
    yield solution_id
    del os.environ["SOLUTION_ID"]


# Added before invalid version to valid version test as we removed the version check as we also test mainline in NW.
@pytest.fixture(
    params=[
        "v1.0.0-alpha",
        "v1.0.0-alpha.1",
        "v1.0.0-alpha.beta",
        "v1.0.0-beta",
        "v1.0.0-beta.2",
        "v1.0.0-beta.11",
        "v1.0.0-rc.1",
        "v1.0.0",
        "v1.0.0-alpha",
        "v1.0.0-alpha.1",
        "v1.0.0-0.3.7",
        "v1.0.0-x.7.z.92",
        "v1.0.0-x-y-z.-",
        "v1.0.0-alpha+001",
        "v1.0.0+20130313144700",
        "v1.0.0-beta+exp.sha.5114f85",
        "v1.0.0+21AF26D3--117B344092BD",
        "a.b.c",
        "a1.2.3",
        "v.1.2.3",
        "mainline"
    ]
)
def solution_version_valid(request):
    solution_version = request.param
    os.environ["SOLUTION_VERSION"] = solution_version
    yield solution_version
    del os.environ["SOLUTION_VERSION"]


def test_valid_solution_id(solution_id_valid):
    config_id = aws_solutions.core.config.id
    assert config_id == solution_id_valid

def test_invalid_solution_id(solution_id_invalid):
    with pytest.raises(ValueError):
        aws_solutions.core.config.id


def test_valid_solution_version(solution_version_valid):
    version = aws_solutions.core.config.version
    assert version == solution_version_valid


# Removing test_invalid_solution_id test as we removed the version check as we also test mainline in NW.

def test_valid_botocore_config(solution_id_valid, solution_version_valid):
    boto_config = aws_solutions.core.config.botocore_config
    assert boto_config.user_agent_extra == f"AwsSolution/{solution_id_valid}/{solution_version_valid}"


def test_solution_config_env_reuse():
    aws_solutions.core.config.id = "SO0100"

    id_1 = aws_solutions.core.config.id
    id_2 = aws_solutions.core.config.id

    assert id_1 is id_2


def test_botocore_config_change():
    aws_solutions.core.config.id = "SO0100"
    aws_solutions.core.config.version = "v1.0.0"
    aws_solutions.core.config.botocore_config.read_timeout = 123
    assert aws_solutions.core.config.botocore_config.read_timeout == 123


def test_botocore_config_change_defaults():
    # it is probably better to just set the value directly as per above
    aws_solutions.core.config.id = "SO0100"
    aws_solutions.core.config.version = "v1.0.0"
    aws_solutions.core.config.botocore_config
    cfg_2 = botocore.config.Config(read_timeout=123)
    aws_solutions.core.config.botocore_config = cfg_2

    assert aws_solutions.core.config.botocore_config.read_timeout == 123
    assert aws_solutions.core.config.botocore_config.user_agent_extra == f"AwsSolution/SO0100/v1.0.0"
