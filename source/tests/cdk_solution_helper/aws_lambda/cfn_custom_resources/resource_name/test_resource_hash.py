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


import pytest

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.resource_hash.src.custom_resources.hash import (
    generate_hash,
    get_property,
    helper,
)

EXPECTED_DIGEST = "DCB88E2D2EC20C11929E7C2C0366FEB6"


@pytest.fixture()
def lambda_event():
    event = {
        "StackId": f"arn:aws:cloudformation:us-west-2:{''.join([str(i % 10) for i in range(1,13)])}:stack/stack-name/guid",
        "ResourceProperties": {
            "Purpose": "set-me",
            "MaxLength": 64,
        },
    }
    yield event


def test_generate_hashed_name(lambda_event):
    generate_hash(lambda_event, None)
    assert helper.Data["Name"] == f"{lambda_event['ResourceProperties']['Purpose']}-{EXPECTED_DIGEST[:8]}"


def test_generate_hashed_name_long(lambda_event):
    lambda_event["ResourceProperties"]["Purpose"] = "a" * (64 - 9)
    generate_hash(lambda_event, None)
    assert helper.Data["Name"] == f"{lambda_event['ResourceProperties']['Purpose']}-{EXPECTED_DIGEST[:8]}"


def test_generate_hashed_name_long(lambda_event):
    lambda_event["ResourceProperties"]["Purpose"] = "a" * (64 - 8)
    with pytest.raises(ValueError):
        generate_hash(lambda_event, None)


def test_get_property_present(lambda_event):
    assert get_property(lambda_event, "MaxLength") == 64


def test_get_property_default(lambda_event):
    assert get_property(lambda_event, "MissingProperty", "DEFAULT") == "DEFAULT"


def test_get_property_missing(lambda_event):
    with pytest.raises(ValueError):
        get_property(lambda_event, "MissingProperty")
