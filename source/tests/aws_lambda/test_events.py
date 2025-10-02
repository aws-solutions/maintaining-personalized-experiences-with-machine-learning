# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzlocal

from shared.events import Notifies
from shared.resource import DatasetGroup


def test_notifies_decorator_create(notifier_stubber):
    status = "ACTIVE"

    class RequiresNotification:
        @Notifies(status=status)
        def notifies_something(self, resource, **kwargs):
            return {"datasetGroupArn": "SOME_ARN"}

    rn = RequiresNotification()
    rn.notifies_something(DatasetGroup(), timeStarted="2021-10-10T10:00:00Z")

    assert notifier_stubber.creation_notifications[0] == {
        "resource": "datasetGroup",
        "status": "ACTIVE",
        "result": {
            "datasetGroupArn": "SOME_ARN",
        },
    }
    assert len(notifier_stubber.creation_notifications) == 1
    assert len(notifier_stubber.completion_notifications) == 0


def test_notifies_decorator_complete(mocker, notifier_stubber):
    status = "ACTIVE"

    created = datetime.now(tzlocal())
    updated = created + relativedelta(seconds=120)

    class RequiresNotification:
        @Notifies(status=status)
        def notifies_something(self, resource, **kwargs):
            return {
                "datasetGroup": {
                    "datasetGroupArn": "SOME_ARN",
                    "creationDateTime": created,
                    "lastUpdatedDateTime": updated,
                    "status": "ACTIVE",
                }
            }

    rn = RequiresNotification()
    rn.notifies_something(DatasetGroup(), timeStarted=created)

    assert notifier_stubber.completion_notifications[0] == {
        "resource": "datasetGroup",
        "result": {
            "datasetGroup": {
                "datasetGroupArn": "SOME_ARN",
                "lastUpdatedDateTime": updated,
                "creationDateTime": created,
                "status": "ACTIVE",
            }
        },
        "status": "ACTIVE",
    }
    assert len(notifier_stubber.creation_notifications) == 0
    assert len(notifier_stubber.completion_notifications) == 1
