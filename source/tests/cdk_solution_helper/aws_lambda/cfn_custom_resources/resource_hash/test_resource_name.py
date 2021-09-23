# #####################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                 #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. You may obtain a copy of the License at                                                          #
#                                                                                                                     #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                        #
#                                                                                                                     #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed   #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for  #
#  the specific language governing permissions and limitations under the License.                                     #
# #####################################################################################################################


import re

import pytest

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.resource_name.src.custom_resources.name import (
    generate_name,
    get_property,
    helper,
)


@pytest.fixture()
def lambda_event():
    event = {
        "ResourceProperties": {
            "Id": "UniqueId",
            "StackName": "StackName",
            "Purpose": "Purpose",
            "MaxLength": 63,
        }
    }
    yield event


def test_generate_name(lambda_event):
    generate_name(lambda_event, None)
    assert helper.Data["Name"] == "stackname-purpose-uniqueid"


def test_generate_long_name(lambda_event):
    lambda_event["ResourceProperties"]["StackName"] = "a" * 63
    generate_name(lambda_event, None)
    assert helper.Data["Name"] == "purpose-uniqueid"


def test_generate_invalid_name(lambda_event):
    lambda_event["ResourceProperties"]["Purpose"] = "a" * 630
    with pytest.raises(ValueError):
        generate_name(lambda_event, None)


def test_generate_name_random_id(lambda_event):
    del lambda_event["ResourceProperties"]["Id"]
    generate_name(lambda_event, None)
    helper_id = helper.Data["Id"]
    assert len(helper_id) == 12
    assert re.match(r"[a-f0-9]{12}", helper_id)


def test_get_property_present(lambda_event):
    assert get_property(lambda_event, "StackName") == "StackName"


def test_get_property_default(lambda_event):
    assert get_property(lambda_event, "MissingProperty", "DEFAULT") == "DEFAULT"


def test_get_property_missing(lambda_event):
    with pytest.raises(ValueError):
        get_property(lambda_event, "MissingProperty")
