# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import dataclasses
import functools

from aws_solutions.scheduler.common.schedule import Schedule
from aws_solutions.scheduler.common.task import Task


class TaskResource:
    """Used as a decorator on AWS Lambda Functions to transform the AWS Lambda Event input as a Task"""

    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func

    def __call__(self, *args, **kwargs):
        task: Task = Task(**args[0])
        task: Task = self.func(task, args[1], **kwargs)

        if not task:
            return None
        else:
            # convert the schedule into a string
            if isinstance(task.schedule, Schedule):
                task.schedule = task.schedule.expression
            return dataclasses.asdict(task)
