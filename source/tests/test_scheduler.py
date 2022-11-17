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

import boto3
import pytest
from moto.core import ACCOUNT_ID
from moto.dynamodb2 import mock_dynamodb2
from moto.stepfunctions import mock_stepfunctions

from aws_solutions.scheduler.cdk.aws_lambda.scheduler.handler import (
    create_schedule,
    read_schedule,
    update_schedule,
    delete_schedule,
)
from aws_solutions.scheduler.common import (
    Scheduler,
    Schedule,
    ScheduleError,
    Task,
)


@pytest.fixture
def scheduler_stepfunctions_target_arn():
    stepfunction_name = "personalizestack-personalize-target"
    stepfunction_arn = f"arn:aws:states:us-east-1:{ACCOUNT_ID}:stateMachine:{stepfunction_name}"
    return stepfunction_arn


@pytest.fixture
def scheduler_stepfunctions_scheduler_arn():
    stepfunction_name = "personalizestack-personalize-scheduler"
    stepfunction_arn = f"arn:aws:states:us-east-1:{ACCOUNT_ID}:stateMachine:{stepfunction_name}"
    return stepfunction_arn


@pytest.fixture
def scheduler_stepfunctions(scheduler_stepfunctions_target_arn, scheduler_stepfunctions_scheduler_arn):
    with mock_stepfunctions():
        sfn = boto3.client("stepfunctions")
        definition = json.dumps(
            {
                "StartAt": "FirstState",
                "States": {
                    "Type": "Task",
                    "Resource": f"arn:aws:lambda:us-east-1:{ACCOUNT_ID}:function:FUNCTION_NAME",
                    "End": True,
                },
            }
        )
        sfn.create_state_machine(
            name=scheduler_stepfunctions_target_arn.split(":")[-1],
            definition=definition,
            roleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/sf_role",
        )
        sfn.create_state_machine(
            name=scheduler_stepfunctions_scheduler_arn.split(":")[-1],
            definition=definition,
            roleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/sf_role",
        )
        yield sfn, scheduler_stepfunctions_target_arn, scheduler_stepfunctions_scheduler_arn


@pytest.fixture
def scheduler_table():
    scheduler_table_name = "scheduler"
    os.environ["DDB_SCHEDULES_TABLE"] = scheduler_table_name

    with mock_dynamodb2():
        ddb = boto3.resource("dynamodb")
        ddb.create_table(
            TableName=scheduler_table_name,
            KeySchema=[
                {"AttributeName": "name", "KeyType": "HASH"},
                {
                    "AttributeName": "version",
                    "KeyType": "RANGE",
                },
            ],
            AttributeDefinitions=[
                {"AttributeName": "name", "AttributeType": "S"},
                {"AttributeName": "version", "AttributeType": "S"},
            ],
        )

        yield ddb, scheduler_table_name


@pytest.fixture
def task(scheduler_stepfunctions_target_arn):
    return Task(
        name="test",
        schedule="cron(* * * * ? *)",
        state_machine={"arn": scheduler_stepfunctions_target_arn, "input": {}},
    )


@pytest.fixture
def scheduler(scheduler_table, scheduler_stepfunctions, mocker):
    _, scheduler_table_name = scheduler_table
    sfn_cli, _, sfn_arn = scheduler_stepfunctions

    _scheduler = Scheduler()
    _scheduler.sfn_cli = sfn_cli
    _scheduler.stepfunction = sfn_arn
    mocker.patch("aws_solutions.scheduler.cdk.aws_lambda.scheduler.handler.scheduler", _scheduler)

    yield _scheduler


def test_create(scheduler, task):
    scheduler.create(task)
    scheduled = scheduler.read(task.name)
    assert scheduled == task


def test_read(scheduler, task):
    scheduler.create(task)
    scheduler.update(task)

    scheduled = scheduler.read(task)
    assert scheduled.latest == 1
    assert scheduled.version == "v0"


def test_delete(scheduler, task):
    scheduler.create(task)
    scheduler.update(task)
    scheduler.delete(task)

    assert not scheduler.read(task)  # the updated item should no longer be present


def test_list(scheduler, task):
    # create two tasks, then list them
    scheduler.create(task)
    scheduler.update(task)
    task.name = "test1"
    task.next_task_id = task.get_next_task_id()
    scheduler.create(task)
    scheduler.update(task)

    schedules = [s for s in scheduler.list()]
    assert len(schedules) == 2
    assert "test" in schedules
    assert "test1" in schedules


def test_scheduler_create_handler(scheduler, scheduler_stepfunctions_target_arn):
    create_schedule(
        {
            "name": "test",
            "schedule": "cron(* * * * ? *)",
            "state_machine": {
                "arn": scheduler_stepfunctions_target_arn,
                "input": {},
            },
        },
        None,
    )


def test_scheduler_update_handler(task, scheduler, scheduler_stepfunctions_target_arn):
    scheduler.create(task)
    assert scheduler.read(task).schedule == task.schedule

    new_schedule = Schedule("cron(10 * * * ? *)")
    update_schedule(
        {
            "name": "test",
            "schedule": new_schedule.expression,
            "state_machine": {
                "arn": scheduler_stepfunctions_target_arn,
                "input": {},
            },
        },
        None,
    )
    assert scheduler.read(task).schedule == new_schedule
    assert scheduler.read(task).latest == 2


def test_read_schedule_handler(task, scheduler):
    scheduler.create(task)
    result = read_schedule(
        {
            "name": "test",
        },
        None,
    )

    assert result.get("name") == task.name
    assert result.get("schedule") == task.schedule.expression


def test_delete_schedule_handler(task, scheduler):
    scheduler.create(task)

    assert scheduler.read(task.name)
    delete_schedule(
        {
            "name": "test",
        },
        None,
    )
    assert not scheduler.read(task.name)


def test_delete_as_create(scheduler):
    task = Task("testing", schedule="delete")

    scheduler.create(task)
    assert not scheduler.read(task.name)


@pytest.mark.parametrize(
    "expression",
    [
        "cron(0 12 * * ? * *)",  # too many fields
        "cron(0 12 * * ?)",  # too few fields
        "cron(5,35 14 * * * *)",  # both day of week and day of month specified
        "cron(5,35 14 * * ? 1888)",  # year too early
        "not-cron",  # not a cron expression
    ],
)
def test_configuration_cron_invalid(expression):
    with pytest.raises(ScheduleError):
        Schedule(expression)
