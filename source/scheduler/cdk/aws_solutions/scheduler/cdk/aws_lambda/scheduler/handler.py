# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.utilities.typing import LambdaContext

from aws_solutions.scheduler.common import (
    Scheduler,
    Task,
    TaskResource,
)

logger = Logger()
tracer = Tracer()
scheduler = Scheduler()
metrics = Metrics(service="Scheduler")


@metrics.log_metrics
@tracer.capture_lambda_handler
@TaskResource
def create_schedule(task: Task, _: LambdaContext) -> Dict:
    return scheduler.create(task)


@metrics.log_metrics
@tracer.capture_lambda_handler
@TaskResource
def read_schedule(task: Task, _: LambdaContext) -> Dict:
    return scheduler.read(task)


@metrics.log_metrics
@tracer.capture_lambda_handler
@TaskResource
def update_schedule(task: Task, _: LambdaContext) -> Dict:
    return scheduler.update(task)


@metrics.log_metrics
@tracer.capture_lambda_handler
@TaskResource
def delete_schedule(task: Task, _: LambdaContext) -> Dict:
    return scheduler.delete(task)
