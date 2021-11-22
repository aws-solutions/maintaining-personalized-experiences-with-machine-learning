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

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Union, Dict
from uuid import uuid4

from aws_solutions.scheduler.common import TASK_PK, TASK_SK
from aws_solutions.scheduler.common.schedule import Schedule


@dataclass
class Task:
    """Represents a Scheduler scheduled task"""

    name: str
    schedule: Union[None, str, Schedule] = ""
    state_machine: Dict = field(default_factory=dict, repr=False)
    latest: Decimal = field(default=Decimal(0), repr=False, compare=False)
    version: str = field(default="v0", repr=False, compare=False)
    next_task_id: str = field(repr=False, compare=False, init=False)

    def __post_init__(self):
        if self.schedule:
            self.schedule = Schedule(self.schedule)
        self.next_task_id = self.get_next_task_id()

    def __str__(self) -> str:
        rv = f"{self.name}"
        if self.schedule:
            rv = f"{rv} ({self.schedule.expression})"
        return rv

    def get_next_task_id(self) -> str:
        """
        Get a random next task ID (max 80 characters length)
        :return:
        """
        return f"{self.name[:67]}-{uuid4().hex[:12]}"

    @staticmethod
    def key(task: Union[Task, str], version: int = 0) -> Dict:
        """
        Get the dynamo db key associated with this task
        :param task: the full task name
        :param version: the task version key to request (defaults to 0, the latest task)
        :return: the key
        """
        if isinstance(task, Task):
            task_name = task.name
        elif isinstance(task, str):
            task_name = task
        else:
            raise ValueError("task must be a string or a Task")

        return {TASK_PK: task_name, TASK_SK: f"v{version}"}
