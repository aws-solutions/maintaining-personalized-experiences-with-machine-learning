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
from typing import List

from aws_cdk.aws_stepfunctions import (
    StateMachineFragment,
    State,
    INextable,
    Fail,
    TaskInput,
)
from constructs import Construct

from personalize.sns.notifications import Notifications


class FailureFragment(StateMachineFragment):
    def __init__(
        self,
        scope: Construct,
        notifications: Notifications,
        construct_id: str = "Failure",
    ):
        if construct_id != "Failure":
            construct_id = " ".join([construct_id, "Failure"]).strip()
        super().__init__(scope, construct_id)

        self.failure_state = Fail(self, construct_id)

        self.notification_state = notifications.state(
            self,
            construct_id=f"Send {construct_id} Message",
            payload=TaskInput.from_object(
                {
                    "datasetGroup.$": "$.datasetGroup.serviceConfig.name",
                    "statesError.$": "$.statesError",
                }
            ),
        ).next(self.failure_state)

    @property
    def start_state(self) -> State:
        return self.notification_state

    @property
    def end_states(self) -> List[INextable]:
        return [self.failure_state]
