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
from datetime import datetime, timedelta
from typing import Dict

import pytest

from shared.notifiers.base import Notifier
from shared.resource import Resource, Campaign


class NotifierName(Notifier):
    def notify_create(self, status: str, resource: Resource, result: Dict) -> None:
        pass

    def notify_complete(self, status: str, resource: Resource, result: Dict):
        pass


@pytest.fixture
def notifier():
    return NotifierName()


def test_notify_name(notifier):
    assert notifier.name == "NotifierName"


def test_set_cutoff(notifier):
    now = datetime.now()
    notifier.set_cutoff(now)
    assert notifier.cutoff == now


@pytest.mark.parametrize(
    "resource,result,is_create",
    [
        [Resource(), {"resourceArn": "arn"}, True],
        [Resource(), {"resource": {"resourceArn": "arn"}}, False],
    ],
)
def test_is_create(notifier, resource, result, is_create):
    assert notifier._is_create(resource, result) == is_create


@pytest.mark.parametrize(
    "resource,result,is_stable",
    [
        [Resource(), {"resource": {}}, False],
        [
            Resource(),
            {
                "resource": {
                    "lastUpdatedDateTime": datetime.now(),
                    "creationDateTime": datetime.now(),
                }
            },
            False,
        ],
        [
            Campaign(),
            {
                "campaign": {
                    "lastUpdatedDateTime": datetime.now(),
                    "creationDateTime": datetime.now(),
                    "status": "ACTIVE",
                    "latestCampaignUpdate": {"status": "UPDATING"},
                }
            },
            False,
        ],
        [
            Resource(),
            {
                "resource": {
                    "lastUpdatedDateTime": datetime.now(),
                    "creationDateTime": datetime.now(),
                }
            },
            False,
        ],
    ],
)
def test_is_stable(notifier, resource, result, is_stable):
    notifier.set_cutoff(datetime.now() - timedelta(seconds=100))
    assert notifier._resource_stable(resource, result) == is_stable


@pytest.mark.parametrize(
    "resource,result",
    [
        [Resource(), {"resourceArn": "ARN"}],
        [Resource(), {"resource": {"resourceArn": "ARN"}}],
    ],
)
def test_get_resource_arn(notifier, resource, result):
    assert notifier.get_resource_arn(resource, result) == "ARN"


def test_get_resource_value_error(notifier):
    with pytest.raises(ValueError):
        notifier.get_resource_arn(Resource(), {})
