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

from pathlib import Path
from typing import Optional

import aws_cdk.aws_iam as iam
from aws_cdk.aws_dynamodb import ITable
from aws_cdk.aws_stepfunctions import IChainable
from constructs import Construct

from aws_solutions.cdk.stepfunctions.solutionstep import SolutionStep


class UpdateScheduledTask(SolutionStep):
    def __init__(
        self,  # NOSONAR (python:S107) - allow large number of method parameters
        scope: Construct,
        id: str,
        layers=None,
        failure_state: Optional[IChainable] = None,
        scheduler_table: ITable = None,
        state_machine_arn: str = None,
        state_machine_executions_arn: str = None,
    ):
        self.scheduler_table = scheduler_table
        self.state_machine_arn = state_machine_arn
        self.state_machine_executions_arn = state_machine_executions_arn

        super().__init__(
            scope,
            id,
            layers=layers,
            failure_state=failure_state,
            function="update_schedule",
            entrypoint=Path(__file__).parents[1].resolve() / "aws_lambda" / "scheduler" / "handler.py",
        )

    def _set_permissions(self):
        self.function.add_environment("DDB_SCHEDULER_STEPFUNCTION", self.state_machine_arn)
        self.function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "states:StartExecution",
                    "states:ListExecutions",
                    "states:StopExecution",
                    "states:DescribeExecution",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    self.state_machine_arn,
                    self.state_machine_executions_arn,
                ],
            )
        )

        self.scheduler_table.grant_read_write_data(self.function)
        self.function.add_environment("DDB_SCHEDULES_TABLE", self.scheduler_table.table_name)
