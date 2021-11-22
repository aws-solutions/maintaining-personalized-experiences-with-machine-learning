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

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict

import jmespath
from aws_lambda_powertools import Logger

from shared.resource import Resource

logger = Logger()


ACTIVE = "ACTIVE"
TIME_FMT = "{name}.latestCampaignUpdate.{date} || {name}.{date}"


class Notifier(ABC):
    """Notifiers provide notify_create and notify_complete against a resource and its data"""

    cutoff: datetime
    notified: bool = False

    @abstractmethod
    def notify_create(self, status: str, resource: Resource, result: Dict) -> None:
        """
        Notify for resource creation
        :param status: the status of the resource (usually CREATING, sometimes UPDATING)
        :param resource: the Resource
        :param result: the service response (per a create or update call)
        :return: None
        """
        pass

    @abstractmethod
    def notify_complete(self, status: str, resource: Resource, result: Dict):
        """
        Notify for resource completion
        :param status: the status of the resource (usually ACTIVE)
        :param resource: the Resource
        :param result: the servie response (per a describe call)
        :return: None
        """
        pass

    @property
    def name(self):
        """
        Get the name of the notifier
        :return: str
        """
        return self.__class__.__name__

    def notify(self, status: str, resource: Resource, result: Dict) -> None:
        """
        Top-level notification
        :param status: the resource status
        :param resource: the Resource
        :param result: the resource as returned from the SDK
        :return: None
        """
        logger.debug(f"{resource.name.camel} status update ({status}) on {result}")

        if self._is_create(resource, result):
            logger.info(
                f"notifier {self.name} starting for creation of {resource.name.camel}"
            )
            self.notify_create(status, resource, result)
            self.notified = True
        elif self._resource_stable(resource, result):
            logger.info(
                f"notifier {self.name} starting for completion of {resource.name.camel}"
            )
            self.notify_complete(status, resource, result)
            self.notified = True

    def set_cutoff(self, cutoff: datetime) -> None:
        """
        Sets the cutoff for notification (if the event is received after the cutoff - notify)
        :param cutoff: the cutoff time
        :return: Non e
        """
        self.cutoff = cutoff

    def _is_create(self, resource: Resource, result: Dict) -> bool:
        """
        Checks if the resource is a create or update
        :param resource: the Resource
        :param result: the resource as returned from the SDK
        :return: bool
        """
        if f"{resource.name.camel}Arn" in result.keys():
            return True
        else:
            return False

    def _resource_stable(self, resource: Resource, result: Dict) -> bool:
        """
        Check whether the resource has stabilized and should trigger notification
        :param resource: the Resource
        :param result: the resource as returned from the SDK
        :return: bool
        """
        last_updated = self.get_resource_last_updated(resource, result)
        created = self.get_resource_created(resource, result)
        status = self.get_resource_status(resource, result)
        latest_campaign_update = self.get_resource_latest_campaign_update(
            resource, result
        )

        if not last_updated or not created:
            logger.info(
                f"{resource.name.camel} is not ready for notification (missing lastUpdated or creation DateTime)"
            )
            return False
        elif status != ACTIVE:
            logger.info(f"{resource.name.camel} is not yet {ACTIVE}")
            return False
        elif (
            resource.name.camel == "campaign"
            and latest_campaign_update
            and latest_campaign_update.get("status") != ACTIVE
        ):
            logger.info(f"{resource.name.camel} is updating, and not yet active")
            return False
        elif not self.cutoff:
            logger.debug(
                f"{resource.name.camel} has no cutoff specified for notification"
            )
            return False
        elif last_updated <= self.cutoff:
            logger.info(f"{resource.name.camel} does not require update at this time")
            return False
        else:
            logger.info(f"{resource.name.camel} is ready for notification")
            return True

    def get_resource_latest_campaign_update(
        self, resource: Resource, result: Dict
    ) -> Dict:
        """
        Campaigns track their update status separately from the top-level status - return the update status
        :param resource: the Campaign resource
        :param result: the Campaign as returned from the SDK
        :return: Dict
        """
        return result[resource.name.camel].get("latestCampaignUpdate", {})

    def get_resource_created(self, resource: Resource, result: Dict) -> datetime:
        """
        Get the time of resource creation
        :param resource: the Resource
        :param result: the resource as returned from the SDK
        :return: datetime
        """
        return jmespath.search(
            TIME_FMT.format(name=resource.name.camel, date="creationDateTime"), result
        )

    def get_resource_last_updated(self, resource: Resource, result: Dict) -> datetime:
        """
        Get the time of resource update
        :param resource: the Resource
        :param result: the resource as returned from the SDK
        :return: datetime
        """
        return jmespath.search(
            TIME_FMT.format(name=resource.name.camel, date="lastUpdatedDateTime"),
            result,
        )

    def get_resource_status(self, resource, result: Dict) -> str:
        """
        Get the resource status
        :param resource: the Resource
        :param result: the resource as returned from the SDK
        :return: str
        """
        return result[resource.name.camel].get("status")

    def get_resource_arn(self, resource: Resource, result: Dict) -> str:
        """
        Get the resource ARN
        :param resource: the Resource
        :param result: the resource as returned from the sdk
        :return: str
        """
        arn_key = f"{resource.name.camel}Arn"

        if resource.name.camel in result.keys():
            return result[resource.name.camel][arn_key]
        elif arn_key in result.keys():
            return result[arn_key]
        else:
            raise ValueError("requires a valid SDK response")
