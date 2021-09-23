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

import json
import os
from typing import Dict, Generator, Union, Optional

from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit

from aws_solutions.core import get_service_client, get_service_resource
from shared.scheduler import TASK_PK
from shared.scheduler.task import Task

logger = Logger()
metrics = Metrics(service="Scheduler")


def dynamo_to_python(obj: Optional[Dict]) -> Optional[Task]:
    # if no object was provided, there is nothing to convert
    if not obj:
        return None

    # unpack the flattened state_machine parameters and return the task
    state_machine = {
        "arn": obj.pop("state_machine_arn"),
        "input": obj.pop("state_machine_input"),
    }
    obj["state_machine"] = state_machine

    task = Task(**obj)
    return task


class Scheduler:
    """Create schedules for events that invoke a step function"""

    def __init__(self):
        self.ddb = get_service_resource("dynamodb")
        self.ddb_cli = self.ddb.meta.client
        self.table_name = os.environ.get("DDB_SCHEDULES_TABLE")
        self.stepfunction = os.environ.get("DDB_SCHEDULER_STEPFUNCTION")
        self.sfn_cli = get_service_client("stepfunctions")
        self.table = self.ddb.Table(self.table_name)

    def create(self, task: Task) -> Optional[Task]:
        """
        Create a new task and its associated schedule and start the waiter
        :param task: the task to schedule
        :return: None
        """
        logger.info(f"creating scheduled task for {task}")

        if task.schedule.expression == "delete":
            self.delete(task)
            return

        changed = self._transact_put(task)

        if changed:
            logger.info(f"enabling scheduled task for {task}")
            self._enable_schedule(task)

        return task

    def read(self, task: Union[Task, str], version=0) -> Optional[Task]:
        """
        Read the latest task from the table
        :param task: the task to read
        :param version: the task version (0 is always the latest version)
        :return: Task
        """
        task = Task(task) if isinstance(task, str) else task
        try:
            task = self.ddb_cli.get_item(
                TableName=self.table.name,
                Key=Task.key(task, version),
                ConsistentRead=True,
            )
        except self.ddb_cli.exceptions.ResourceNotFoundException:
            return None
        item = task.get("Item")
        task = dynamo_to_python(item)
        return task

    def update(self, task: Task) -> Optional[Task]:
        """
        Update the task and its associated schedule
        :param task: the Task to update
        :return: Task
        """
        logger.info(f"updating scheduled task for {task}")

        if task.schedule == "delete":
            self.delete(task)
            return

        changed = self._transact_put(task)
        if changed:
            logger.info(f"enabling scheduled task for {task}")
            self._enable_schedule(task)

        return task

    def delete(self, task: Task) -> Optional[Task]:
        latest_task = self.read(task)
        versions = 0 if not latest_task else int(latest_task.latest)

        logger.info(f"disabling {task.name}")
        self._disable_schedule(task)

        if not versions:
            logger.info(f"no versions of task {task.name} to remove")
            return None

        logger.info(f"removing all {versions} task(s) for {task.name}")
        with self.table.batch_writer() as batch:
            for i in range(versions + 1):
                batch.delete_item(Key=Task.key(task, i))

        metrics.add_metric("JobsDeleted", unit=MetricUnit.Count, value=1)
        return task

    def list(self) -> Generator[str, None, None]:
        """
        List the managed schedules
        :return: Generator[str] of the schedules (by name)
        """
        done = False
        scan_kwargs = {"ProjectionExpression": TASK_PK}
        start_key = None
        discovered = set()
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = self.table.scan(**scan_kwargs)
            items = response.get("Items", [])
            for item in items:
                item = item[TASK_PK]
                if item not in discovered:
                    discovered.add(item)
                    yield item
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    def _get_running_execution_arn(self, task: Task) -> Optional[str]:
        paginator = self.sfn_cli.get_paginator("list_executions")
        iterator = paginator.paginate(
            stateMachineArn=self.stepfunction, statusFilter="RUNNING"
        )
        for page in iterator:
            executions = page.get("executions", [])
            for execution in executions:
                execution_name = execution["name"]
                execution_arn = execution["executionArn"]

                # since the task name might be truncated in the execution ID we need to describe
                # the execution input to get the full name. Try to avoid long/ duplicate task names
                # in the first 67 characters of the task name for performance reasons
                if execution_name.startswith(task.name[:67]):
                    schedule_input = json.loads(
                        self.sfn_cli.describe_execution(
                            executionArn=execution_arn,
                        )["input"]
                    )
                    schedule_name = schedule_input.get("name")
                    if schedule_name == task.name:
                        return execution_arn
        return None

    def _disable_schedule(self, task: Task) -> None:
        execution_arn = self._get_running_execution_arn(task)
        if execution_arn:
            self.sfn_cli.stop_execution(
                executionArn=execution_arn,
                error="410",
                cause=f"execution disabled for {task.name}",
            )
            logger.info(f"disabled {task.name}")
        else:
            logger.info(f"{task.name} already disabled")

    def _enable_schedule(self, task: Task) -> None:
        execution_arn = self._get_running_execution_arn(task)
        if execution_arn:
            self.sfn_cli.stop_execution(
                executionArn=execution_arn,
                error="301",
                cause=f"execution superseded by {task.next_task_id}",
            )

        self.sfn_cli.start_execution(
            stateMachineArn=self.stepfunction,
            name=task.next_task_id,
            input=json.dumps(
                {
                    "name": task.name,
                }
            ),
        )

    def _transact_put(self, task: Task) -> bool:
        if not task.schedule or not isinstance(task.schedule.expression, str):
            raise ValueError(
                "to create a task, it must have a schedule (e.g. cron(* * * * ? *)"
            )
        if not isinstance(task.state_machine, dict):
            raise ValueError("task state_machine must be a dictionary")
        if (
            "arn" not in task.state_machine.keys()
            or "input" not in task.state_machine.keys()
        ):
            raise ValueError("task state_machine must have an arn and input")
        if not isinstance(task.state_machine["arn"], str):
            raise ValueError("task state_machine.arn must be a string")
        if not isinstance(task.state_machine["input"], dict):
            raise ValueError("task state_machine.input must be a dictionary")

        latest_task = self.read(task)
        version_curr = 0 if not latest_task else latest_task.latest
        version_next = version_curr + 1

        if version_curr != 0 and task == latest_task:
            logger.info(f"task {task.name} unchanged from version {version_curr}")
            return False

        if version_curr == 0:
            metrics.add_metric("JobsCreated", unit=MetricUnit.Count, value=1)

        self.ddb_cli.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": self.table_name,
                        "Key": Task.key(task, 0),
                        # Conditional write makes the update idempotent here
                        # since the conditional check is on the same attribute
                        # that is being updated.
                        "ConditionExpression": "attribute_not_exists(#latest) OR #latest = :latest",
                        "UpdateExpression": "SET #latest = :version_next, #schedule = :schedule, #state_machine_input = :state_machine_input, #state_machine_arn = :state_machine_arn",
                        "ExpressionAttributeNames": {
                            "#latest": "latest",
                            "#schedule": "schedule",
                            "#state_machine_arn": "state_machine_arn",
                            "#state_machine_input": "state_machine_input",
                        },
                        "ExpressionAttributeValues": {
                            ":latest": version_curr,
                            ":version_next": version_next,
                            ":schedule": task.schedule.expression,
                            ":state_machine_input": task.state_machine.get("input"),
                            ":state_machine_arn": task.state_machine.get("arn"),
                        },
                    }
                },
                {
                    "Put": {
                        "TableName": self.table_name,
                        "Item": {
                            **Task.key(task, version_next),
                            "schedule": task.schedule.expression,
                            "state_machine_input": task.state_machine.get("input"),
                            "state_machine_arn": task.state_machine.get("arn"),
                        },
                    }
                },
            ]
        )
        logger.info(f"put scheduled task for {task.name} with version {version_next}")
        return True
