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
import json
import os
from typing import Dict

from aws_lambda_powertools import Logger

from aws_solutions.core import get_service_client
from shared.notifiers.base import Notifier
from shared.resource import Resource

logger = Logger()


class NotifyEventBridge(Notifier):
    """Provide notifications to EventBridge"""

    def __init__(self):
        self.cli = get_service_client("events")
        super().__init__()

    @property
    def bus(self):
        """
        The event BUS ARN
        :return: str
        """
        return os.environ["EVENT_BUS_ARN"]

    def notify_create(self, status: str, resource: Resource, result: Dict) -> None:
        """
        Notify for the creation of a resource
        :param status: the resource status
        :param resource: the Resource
        :param result: the resource as returned from the SDK
        :return: None
        """
        arn = self.get_resource_arn(resource, result)
        self._notify(status, arn, resource)

    def notify_complete(self, status: str, resource: Resource, result: Dict) -> None:
        """
        Notify for the update of a resource
        :param status: the resource status
        :param resource: the Resource
        :param result: the resource as returned from the SDK
        :return: None
        """
        arn = self.get_resource_arn(resource, result)

        created = self.get_resource_created(resource, result)
        updated = self.get_resource_last_updated(resource, result)

        seconds = int((updated - created).total_seconds())
        self._notify(status, arn, resource, duration=seconds)

    def _notify(
        self, status: str, arn: str, resource: Resource, duration: int = 0
    ) -> None:
        """
        The EventBridge notification implementation
        :param status: the resource status
        :param arn: the resource ARN
        :param resource: the Resource
        :param duration: the time it took the resource to stabilize
        :return: None
        """
        detail = {"Arn": arn, "Status": status}
        if duration:
            detail["Duration"] = duration

        result = self.cli.put_events(
            Entries=[
                {
                    "Source": "solutions.aws.personalize",
                    "Resources": [arn],
                    "DetailType": f"Personalize {resource.name.dash.replace('-', ' ').title()} State Change",
                    "Detail": json.dumps(detail),
                    "EventBusName": self.bus,
                }
            ]
        )
        if result["FailedEntryCount"] > 0:
            for entry in result["Entries"]:
                logger.error(
                    f"EventBridge failure ({entry['ErrorCode']}) {entry['ErrorMessage']}"
                )
