# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
