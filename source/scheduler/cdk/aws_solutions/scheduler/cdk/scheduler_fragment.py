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
import re
from typing import List, Optional, Dict

from aws_cdk.aws_stepfunctions import (
    StateMachineFragment,
    State,
    INextable,
    StateMachine,
    TaskInput,
    Pass,
    Choice,
    Condition,
    JsonPath,
)
from aws_cdk.core import Construct

from aws_solutions.scheduler.cdk.construct import Scheduler


class SchedulerFragment(StateMachineFragment):
    def __init__(
        self,  # NOSONAR (python:S107) - allow large number of method parameters
        scope: Construct,
        schedule_for: str,
        schedule_for_suffix: str,
        scheduler: Scheduler,
        target: StateMachine,
        schedule_path: str,
        schedule_input_path: Optional[str] = "",
        schedule_input: Optional[Dict] = None,
    ):
        construct_id = " ".join(["Schedule", schedule_for]).strip()
        super().__init__(scope, construct_id)

        if not schedule_input_path and not schedule_input:
            raise ValueError(
                "schedule_input_path or schedule_input must be provided, not both"
            )
        schedule_input = schedule_input or schedule_input_path

        schedule_input_key = "input"
        if schedule_input_path:
            schedule_input_key += ".$"

        # set up the schedule name
        schedule_for_task_name = re.sub(r"[^0-9A-Za-z-_]", "-", schedule_for)[:80]
        schedule_for_task_name = (
            f"States.Format('{schedule_for_task_name}-{{}}', {schedule_for_suffix})"
        )

        self.not_required = Pass(self, f"{schedule_for.title()} Schedule Not Required")
        self.create_schedule = scheduler.create_scheduled_task.state(
            self,
            f"Create Schedule For {schedule_for.title()}",
            payload=TaskInput.from_object(
                {
                    "name.$": schedule_for_task_name,
                    "schedule.$": schedule_path,
                    "state_machine": {
                        "arn": target.state_machine_arn,
                        schedule_input_key: schedule_input,
                    },
                }
            ),
            result_path=JsonPath.DISCARD,
        )
        self.start = (
            Choice(self, f"Check if {schedule_for.title()} Schedule Required")
            .when(Condition.is_present(schedule_path), self.create_schedule)
            .otherwise(self.not_required)
        )

    @property
    def start_state(self) -> State:
        return self.start.start_state

    @property
    def end_states(self) -> List[INextable]:
        return [self.not_required, self.create_schedule]
