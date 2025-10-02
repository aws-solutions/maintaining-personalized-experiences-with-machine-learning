# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

TASK_PK = "name"
TASK_SK = "version"
CRON_ANY_WILDCARD = "?"
CRON_MIN_MAX_YEAR = (1970, 2199)


from aws_solutions.scheduler.common.base import Scheduler
from aws_solutions.scheduler.common.schedule import Schedule, ScheduleError
from aws_solutions.scheduler.common.task import Task
from aws_solutions.scheduler.common.task_resource import TaskResource
