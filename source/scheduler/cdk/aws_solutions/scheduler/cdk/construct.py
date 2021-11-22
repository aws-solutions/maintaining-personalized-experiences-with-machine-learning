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
from typing import Union, Dict, List

import aws_cdk.aws_dynamodb as ddb
import aws_cdk.aws_iam as iam
from aws_cdk.aws_lambda import Tracing
from aws_cdk.aws_stepfunctions import (
    StateMachine,
    Chain,
    Wait,
    WaitTime,
    IStateMachine,
    CustomState,
    TaskInput,
    Choice,
    Condition,
)
from aws_cdk.aws_stepfunctions_tasks import LambdaInvoke
from aws_cdk.core import Construct, Aws

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.resource_name import ResourceName
from aws_solutions.cdk.aws_lambda.environment import Environment
from aws_solutions.cdk.aws_lambda.java.function import SolutionsJavaFunction
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from aws_solutions.scheduler.cdk.aws_lambda import (
    CreateScheduledTask,
    ReadScheduledTask,
    UpdateScheduledTask,
    DeleteScheduledTask,
)

TASK_NAME_PATH = "$.name"
TRIGGER_AT_PATH = "$.trigger_at"
SCHEDULE_PATH = "$.task.schedule"


class Scheduler(Construct):
    """
    A Scheduler that leverages AWS Step Functions to invoke other AWS Step Functions on a specified cron() schedule.

    To manage tasks:

    1. add the step function to manage through `grant_invoke`
    2. use the public CRUD methods of the Scheduler to set up Scheduler managed tasks
     - create_scheduled_task.function (from another Step Function, use create_scheduled_task.state)
     - read_scheduled_task.function (from another Step Function, use read_scheduled_task.state)
     - update_scheduled_task.function (from another Step Function, use update_scheduled_task.state)
     - delete_scheduled_task.function (from another Step Function, use delete_scheduled_task.state)
    """

    def __init__(self, scope: Construct, construct_id: str, sync: bool = True):
        """
        Create a scheduler using AWS Step Functions
        :param scope: the scope of this construct
        :param construct_id: the ID of this construct
        :param sync: synchronously invoke the scheduled item (otherwise, set to False for async)
        """
        super().__init__(scope, construct_id)

        self.sync = sync
        self.scheduler_function = self._scheduler_function(scope, "GetNextTimestamp")
        self.scheduler_function_environment = Environment(self.scheduler_function)
        self.scheduler_table = self._scheduler_table(scope)

        self._scheduler_child_state_machines: List[IStateMachine] = []
        self._state_machine_namer = ResourceName(
            self,
            "SchedulerStateMachineName",
            purpose="personalize-scheduler",
            max_length=80,
        )

        # Layers required for the AWS Lambda Functions provisioned by the Scheduler construct
        layer_powertools = PowertoolsLayer.get_or_create(self)
        common_layers = [layer_powertools]

        # CRUD tasks/ states to integrate with the Scheduler
        self.create_scheduled_task = CreateScheduledTask(
            self,
            "create_scheduled_task",
            layers=common_layers,
            scheduler_table=self.scheduler_table,
            state_machine_arn=self.state_machine_arn,
            state_machine_executions_arn=self.state_machine_executions_arn,
        )
        self.read_scheduled_task = ReadScheduledTask(
            self,
            "read_scheduled_task",
            layers=common_layers,
            scheduler_table=self.scheduler_table,
            state_machine_arn=self.state_machine_arn,
        )
        self.update_scheduled_task = UpdateScheduledTask(
            self,
            "update_scheduled_task",
            layers=common_layers,
            scheduler_table=self.scheduler_table,
            state_machine_arn=self.state_machine_arn,
            state_machine_executions_arn=self.state_machine_executions_arn,
        )
        self.delete_scheduled_task = DeleteScheduledTask(
            self,
            "delete_scheduled_task",
            layers=common_layers,
            scheduler_table=self.scheduler_table,
            state_machine_arn=self.state_machine_arn,
        )

        read_scheduled_task_state = self.read_scheduled_task.state(
            self,
            "Load Scheduled Task",
            payload=TaskInput.from_object({"name.$": TASK_NAME_PATH}),
            result_path="$.task",
        )

        get_next_trigger_time = self.get_trigger("Get Next Trigger Time")

        invoke_step_function = (
            Wait(
                self,
                "Wait Until Schedule Trigger",
                time=WaitTime.timestamp_path(TRIGGER_AT_PATH),
            )
            .next(
                CustomState(
                    self,
                    "Invoke Step Function",
                    state_json=self._start_execution_task_json(
                        arn_to_invoke="$.task.state_machine.arn",
                        input="$.task.state_machine.input",
                        fallback=get_next_trigger_time,
                    ),
                )
            )
            .next(get_next_trigger_time)
            .next(
                CustomState(
                    self,
                    "Run Next Scheduled Task",
                    state_json=self._start_execution_task_json(
                        arn_to_invoke=self.state_machine_arn,
                        input={
                            "name.$": TASK_NAME_PATH,
                            "trigger_at.$": TRIGGER_AT_PATH,
                        },
                        allow_sync=False,
                    ),
                )
            )
        )

        choice_get_next_trigger = Choice(self, "Trigger Time Provided?")
        choice_get_next_trigger.when(
            Condition.is_not_present(TRIGGER_AT_PATH),
            self.get_trigger("Get Trigger Time"),
        )
        choice_get_next_trigger.afterwards().next(invoke_step_function)
        choice_get_next_trigger.otherwise(invoke_step_function)

        self._scheduler_definition = Chain.start(
            read_scheduled_task_state.next(choice_get_next_trigger)
        )

        self.state_machine = StateMachine(
            self,
            "SchedulerStateMachine",
            state_machine_name=self.state_machine_name,
            definition=self._scheduler_definition,
            tracing_enabled=True,
        )

    def grant_invoke(self, state_machine: IStateMachine) -> None:
        """
        Allow the Scheduler to start executions of the provided state machine
        :param state_machine: The state machine that the scheduler will start executions of
        :return: None
        """
        self._scheduler_child_state_machines.append(state_machine)

    def get_trigger(self, construct_id: str) -> LambdaInvoke:
        """
        Get a task that returns the next trigger time from a cron schedule at $.schedule
        :param construct_id: The name of the task
        :return: the LambdaInvoke Task
        """
        return LambdaInvoke(
            self,
            construct_id,
            lambda_function=self.scheduler_function,
            payload=TaskInput.from_object(
                {
                    "schedule.$": SCHEDULE_PATH,
                }
            ),
            result_path=TRIGGER_AT_PATH,
            payload_response_only=True,
        )

    def _start_execution_task_json(
        self,
        arn_to_invoke: str,
        input: Union[str, Dict],
        fallback: LambdaInvoke = None,
        allow_sync: bool = True,
    ) -> Dict:
        """
        Helper method to prepare the task input data for a states:startExecution task
        :param arn_to_invoke: the state machine ARN to invoke
        :param input: the input to provide to the state machine
        :param allow_sync: whether to run sync or async (default: sync)
        :param next: the next task to run
        :return: Dict of the task properties
        """
        state_machine_arn_property = "StateMachineArn"
        if arn_to_invoke.startswith("$."):
            state_machine_arn_property += ".$"

        input_property = "Input"
        if isinstance(input, str) and input.startswith("$."):
            input_property += ".$"

        if allow_sync and self.sync:
            resource = "arn:aws:states:::states:startExecution.sync:2"
        else:
            resource = "arn:aws:states:::states:startExecution"

        task_json = {
            "Type": "Task",
            "Resource": resource,
            "Parameters": {
                state_machine_arn_property: arn_to_invoke,
                input_property: input,
                "Name.$": "$.task.next_task_id",
            },
            "Retry": [{"ErrorEquals": ["StepFunctions.ExecutionLimitExceeded"]}],
            "ResultPath": "$.startExecutionResult",  # must output ; https://github.com/aws/aws-cdk/issues/8754
            "ResultSelector": {"ExecutionArn.$": "$.ExecutionArn"},
        }
        if fallback:
            task_json["Catch"] = [
                {
                    "ErrorEquals": ["States.TaskFailed"],
                    "Next": fallback.id,
                    "ResultPath": "$.startExecutionResult",
                }
            ]
        return task_json

    @property
    def state_machine_arn(self) -> str:
        """
        Gets the state machine ARN for the scheduler state machine
        :return: str
        """
        return f"arn:{Aws.PARTITION}:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:{self.state_machine_name}"

    @property
    def state_machine_executions_arn(self) -> str:
        return f"arn:{Aws.PARTITION}:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:{self.state_machine_name}:*"

    @property
    def state_machine_name(self) -> str:
        """
        Gets the state machine name for the scheduler state machine
        :return: str
        """
        return self._state_machine_namer.resource_name.to_string()

    def _prepare(self) -> None:
        """
        Finalize/ prepare the state machine and associated permissions
        :return: None
        """
        # permision: allow the scheduler to call itself
        self.state_machine.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[self.state_machine_arn],
                actions=["states:StartExecution"],
            )
        )
        if self.sync:
            self.state_machine.add_to_role_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                    actions=["states:DescribeExecution", "states:StopExecution"],
                )
            )
            self.state_machine.add_to_role_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:{Aws.PARTITION}:events:{Aws.REGION}:{Aws.ACCOUNT_ID}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"
                    ],
                    actions=[
                        "events:PutTargets",
                        "events:PutRule",
                        "events:DescribeRule",
                    ],
                )
            )

        add_cfn_nag_suppressions(
            self.state_machine.role.node.try_find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                CfnNagSuppression(
                    "W12",
                    "IAM policy for nested synchronous invocation of step functions requires * on Describe and Stop Execution",
                ),
                CfnNagSuppression(
                    "W76",
                    "Large step functions need larger IAM roles to access all managed AWS Lambda functions",
                ),
            ],
        )

        # permission: allow the scheduler to call its referenced children
        for child in self._scheduler_child_state_machines:
            child.grant_start_execution(self.state_machine)

    def _scheduler_table(self, scope: Construct) -> ddb.Table:
        """
        Creates the table for tracking scheduled tasks managed by this Scheduler
        :param scope: the scope of the construct (the scheduler)
        :return:
        """
        tasks_table = ddb.Table(
            scope,
            "ScheduledTasks",
            point_in_time_recovery=True,
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            encryption=ddb.TableEncryption.AWS_MANAGED,
            partition_key=ddb.Attribute(
                name="name",
                type=ddb.AttributeType.STRING,
            ),
            sort_key=ddb.Attribute(
                name="version",
                type=ddb.AttributeType.STRING,
            ),
        )
        tasks_table.node.default_child.override_logical_id("PersonalizeScheduledTasks")

        return tasks_table

    def _scheduler_function(
        self, scope: Construct, construct_id: str
    ) -> SolutionsJavaFunction:
        """
        Creates the AWS Lambda Function for getting the next scheduled task time from a cron expression
        :param scope: the scope of the function
        :param construct_id: the construct ID of the function
        :return: SolutionsJavaFunction
        """
        project_path = (
            Path(__file__).absolute().parents[1]
            / "cdk"
            / "aws_lambda"
            / "get_next_scheduled_event"
        )
        distribution_path = project_path / "build" / "distributions"

        function = SolutionsJavaFunction(
            scope=scope,
            construct_id=construct_id,
            handler="com.amazonaws.solutions.schedule_sfn_task.HandleScheduleEvent",
            project_path=project_path,
            gradle_task="buildZip",
            gradle_test="test",
            distribution_path=distribution_path,
            tracing=Tracing.ACTIVE,
        )
        add_cfn_nag_suppressions(
            function.role.node.try_find_child("DefaultPolicy").node.find_child(
                "Resource"
            ),
            [
                CfnNagSuppression(
                    "W12", "IAM policy for AWS X-Ray requires an allow on *"
                )
            ],
        )
        return function
