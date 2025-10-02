# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List

from aws_cdk import Duration
from aws_cdk.aws_stepfunctions import (
    StateMachineFragment,
    State,
    INextable,
    Choice,
    Pass,
    Condition,
)
from constructs import Construct

from personalize.aws_lambda.functions import (
    CreateEventTracker,
)


class EventTrackerFragment(StateMachineFragment):
    def __init__(
        self,
        scope: Construct,
        id: str,
        create_event_tracker: CreateEventTracker,
    ):
        super().__init__(scope, id)

        # total allowed elapsed duration ~ 11m30s
        retry_config = {
            "backoff_rate": 1.25,
            "interval": Duration.seconds(8),
            "max_attempts": 15,
        }

        self.create_event_tracker = create_event_tracker.state(
            self,
            "Create Event Tracker",
            **retry_config,
        )
        self.not_required = Pass(self, "Event Tracker not Required")
        self.start = (
            Choice(self, "Check if Event Tracker Required")
            .when(
                Condition.is_present("$.eventTracker.serviceConfig.name"),
                self.create_event_tracker,
            )
            .otherwise(self.not_required)
        )

    @property
    def start_state(self) -> State:
        return self.start.start_state

    @property
    def end_states(self) -> List[INextable]:
        return [self.not_required, self.create_event_tracker]
