# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
