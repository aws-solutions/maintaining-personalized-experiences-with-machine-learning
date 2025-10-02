# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_cdk.aws_stepfunctions import Chain, DefinitionBody, Parallel, StateMachine, TaskInput
from constructs import Construct
from aws_solutions.cdk.aws_lambda.cfn_custom_resources.resource_name import ResourceName
from aws_solutions.cdk.cfn_nag import CfnNagSuppression, add_cfn_nag_suppressions
from aws_solutions.cdk.stepfunctions.solutionstep import SolutionStep
from personalize.aws_lambda.functions import (
    CreateBatchInferenceJob,
    CreateBatchSegmentJob,
    CreateCampaign,
    CreateRecommender,
    CreateSolution,
    CreateSolutionVersion,
)
from personalize.aws_lambda.functions.prepare_input import PrepareInput
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
        create_batch_segment_job: CreateBatchSegmentJob,
        prepare_input: PrepareInput,
        create_timestamp: SolutionStep,
        notifications: SolutionStep,
        create_recommender: CreateRecommender,
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
            tracing_enabled=True,
            state_machine_name=state_machine_namer.resource_name.to_string(),
            definition_body=DefinitionBody.from_chainable(
                Chain.start(
                    Parallel(self, "Manage Solution Maintenance")
                    .branch(
                        create_timestamp.state(self, "Set Current Timestamp", result_path="$.currentDate")
                        .next(prepare_input.state(self, "Prepare Input"))
                        .next(
                            SolutionFragment(
                                self,
                                "Handle Periodic Solution Maintenance",
                                create_solution=create_solution,
                                create_solution_version=create_solution_version,
                                create_campaign=create_campaign,
                                create_batch_inference_job=create_batch_inference_job,
                                create_batch_segment_job=create_batch_segment_job,
                                create_recommender=create_recommender,
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
                            payload=TaskInput.from_object({"datasetGroup.$": "$[0].datasetGroup.serviceConfig.name"}),
                        )
                    )
                )
            ),
        )
        add_cfn_nag_suppressions(
            self.state_machine.role.node.try_find_child("DefaultPolicy").node.find_child("Resource"),
            [
                CfnNagSuppression("W12", "IAM policy for AWS X-Ray requires an allow on *"),
                CfnNagSuppression(
                    "W76",
                    "Large step functions need larger IAM roles to access all managed AWS Lambda functions",
                ),
            ],
        )
