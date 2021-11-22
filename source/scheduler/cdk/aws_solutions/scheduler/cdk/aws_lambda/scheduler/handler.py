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
