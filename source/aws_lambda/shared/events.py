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
from typing import Dict, Optional

from aws_lambda_powertools import Logger

from shared.exceptions import (
    NotificationError,
    SolutionVersionPending,
)
from shared.notifiers import NotifyEventBridge
from shared.resource import Resource

logger = Logger()


NOTIFY_LIST = [NotifyEventBridge()]


class Notifies:
    """Decorates a resource creation or describe call to provide event notifications"""

    def __init__(self, status: str):
        self.status = status

    def __call__(self, function):
        def wrapper(caller, resource: Resource, **kwargs):
            try:
                result = function(caller, resource, **kwargs)
            except SolutionVersionPending as exc:
                # because of how solution versions are handled, we must manually notify and re-raise
                self.notify(
                    resource=resource,
                    result={
                        "solutionVersionArn": str(exc),
                        "status": "CREATE IN_PROGRESS",
                    },
                    cutoff=None,
                )
                raise exc

            # run the notifier
            cutoff = kwargs.get("timeStarted")
            self.notify(resource, result, cutoff)

            return result

        return wrapper

    def notify(
        self, resource: Resource, result: Dict, cutoff: Optional[datetime]
    ) -> None:
        """
        Notify each target in the NOTIFY_LIST
        :param resource: the subject of the notification
        :param result: the description of the subject of the notification
        :param cutoff: the cutoff datetime for notifications (UTC required, timezone aware)
        :return: None
        """
        for notifier in NOTIFY_LIST:
            notifier.set_cutoff(cutoff)
            try:
                notifier.notify(self.status, resource, result)
            except NotificationError as exc:
                logger.error(
                    f"notifier {notifier.name} failed: {str(exc)}"
                )  # log and continue through notifiers
