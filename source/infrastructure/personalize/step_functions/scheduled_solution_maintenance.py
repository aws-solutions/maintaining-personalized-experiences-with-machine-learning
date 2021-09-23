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

from aws_cdk.aws_stepfunctions import StateMachine, Chain, Parallel, TaskInput
from aws_cdk.core import Construct

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.resource_name import ResourceName
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from personalize.aws_lambda.functions import (
    CreateBatchInferenceJob,
    CreateSolution,
    CreateSolutionVersion,
    CreateCampaign,
)
from personalize.aws_lambda.functions.solutionstep import SolutionStep
from personalize.step_functions.failure_fragment import FailureFragment
from personalize.step_functions.solution_fragment import SolutionFragment


class ScheduledSolutionMaintenance(Construct):
    def __init__(
        self,  # NOSONAR (python:S107) - allow large number of method parameters
        scope: Construct,
        construct_id: str,
        create_solution: CreateSolution,
        create_solution_version: CreateSolutionVersion,
        create_campaign: CreateCampaign,
        create_batch_inference_job: CreateBatchInferenceJob,
        create_timestamp: SolutionStep,
        notifications: SolutionStep,
    ):
        super().__init__(scope, construct_id)

        state_machine_namer = ResourceName(
            self,
            "StateMachineName",
            purpose="periodic-solution-maintenance",
            max_length=80,
        )
        self.state_machine = StateMachine(
            self,
            "PeriodicSolutionMaintenance",
            state_machine_name=state_machine_namer.resource_name.to_string(),
            definition=Chain.start(
                Parallel(self, "Manage Solution Maintenance")
                .branch(
                    create_timestamp.state(
                        self, "Set Current Timestamp", result_path="$.currentDate"
                    ).next(
                        SolutionFragment(
                            self,
                            "Handle Periodic Solution Maintenance",
                            create_solution=create_solution,
                            create_solution_version=create_solution_version,
                            create_campaign=create_campaign,
                            create_batch_inference_job=create_batch_inference_job,
                        )
                    )
                )
                .add_catch(
                    FailureFragment(self, notifications).start_state,
                    errors=["States.ALL"],
                    result_path="$.statesError",
                )
                .next(
                    notifications.state(
                        self,
                        "Success",
                        payload=TaskInput.from_object(
                            {"datasetGroup.$": "$[0].datasetGroup.serviceConfig.name"}
                        ),
                    )
                )
            ),
        )
        add_cfn_nag_suppressions(
            self.state_machine.role.node.try_find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                CfnNagSuppression(
                    "W12", "IAM policy for AWS X-Ray requires an allow on *"
                ),
                CfnNagSuppression(
                    "W76",
                    "Large step functions need larger IAM roles to access all managed AWS Lambda functions",
                ),
            ],
        )
