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
