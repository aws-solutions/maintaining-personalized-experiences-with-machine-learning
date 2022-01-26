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
from typing import List, Optional

from aws_cdk import Duration
from aws_cdk.aws_stepfunctions import (
    StateMachineFragment,
    State,
    INextable,
    Choice,
    Pass,
    Map,
    Condition,
    JsonPath,
    Parallel,
    StateMachine,
)
from constructs import Construct

from aws_solutions.scheduler.cdk.construct import Scheduler
from personalize.aws_lambda.functions import (
    CreateSolution,
    CreateSolutionVersion,
    CreateCampaign,
    CreateBatchInferenceJob,
    CreateBatchSegmentJob,
    CreateRecommender,
)
from personalize.step_functions.batch_inference_jobs_fragment import (
    BatchInferenceJobsFragment,
)
from personalize.step_functions.batch_segment_jobs_fragment import (
    BatchSegmentJobsFragment,
)
from personalize.step_functions.scheduler_fragment import SchedulerFragment

TEMPORARY_PATH = "$._tmp"
BUCKET_PATH = "$.bucket"
CURRENT_DATE_PATH = "$.currentDate"
MINIMUM_TIME = "1 second"


class SolutionFragment(StateMachineFragment):
    def __init__(
        self,  # NOSONAR (python:S107) - allow large number of method parameters
        scope: Construct,
        id: str,
        create_solution: CreateSolution,
        create_solution_version: CreateSolutionVersion,
        create_campaign: CreateCampaign,
        create_batch_inference_job: CreateBatchInferenceJob,
        create_batch_segment_job: CreateBatchSegmentJob,
        create_recommender: CreateRecommender,
        scheduler: Optional[Scheduler] = None,
        to_schedule: Optional[StateMachine] = None,
    ):
        super().__init__(scope, id)
        self.create_solution = create_solution
        self.create_solution_version = create_solution_version

        # total allowed elapsed duration ~ 5h
        retry_config = {
            "backoff_rate": 1.02,
            "interval": Duration.seconds(60),
            "max_attempts": 100,
        }

        # fmt: off
        self.recommenders_not_available = Pass(self, "Recommenders not Provided")
        self.solutions_not_available = Pass(self, "Solutions not Provided")
        self.recommenders_available = Choice(self, "Check for Recommenders").otherwise(self.recommenders_not_available)
        self.solutions_available = Choice(self, "Check for Solutions").otherwise(self.solutions_not_available)
        campaigns_available = Choice(self, "Check for Campaigns").otherwise(Pass(self, "Campaigns not Provided"))

        _prepare_solution_input = Pass(
            self,
            "Prepare Solution Input Data",
            input_path="$.datasetGroupArn",  # NOSONAR (python:S1192) - string for clarity
            result_path="$.solution.serviceConfig.datasetGroupArn",
        )
        _prepare_recommender_input = Pass(
            self,
            "Prepare Recommender Input Data",
            input_path="$.datasetGroupArn", # NOSONAR (python:S1192) - string for clarity
            result_path="$.recommender.serviceConfig.datasetGroupArn"
        )

        _prepare_solution_output = Pass(
            self,
            "Prepare Solution Output Data",
            input_path=f"{TEMPORARY_PATH}.solutionArn",
            result_path="$.solution.serviceConfig.solutionArn",
        )

        _prepare_solution_version_input = Pass(
            self,
            "Prepare Solution Version Input Data",
            parameters={
                "serviceConfig": {
                    "solutionArn.$": "$.solution.serviceConfig.solutionArn",  # NOSONAR (python:S1192) - string for clarity
                    "trainingMode": "FULL"
                },
                "workflowConfig": {
                    "maxAge": "365 days",  # do not create a new solution version on new file upload
                    "timeStarted.$": "$$.State.EnteredTime",
                }
            },
            result_path = "$.solution.solutionVersion",  # NOSONAR (python:S1192) - string for clarity
        )

        _prepare_solution_version_output = Pass(
            self,
            "Prepare Solution Version Output Data",
            input_path=f"{TEMPORARY_PATH}.solutionVersionArn",
            result_path="$.solution.solutionVersion.serviceConfig.solutionVersionArn",  # NOSONAR (python:S1192) - string for clarity
        )

        _prepare_campaign_input = Pass(
            self,
            "Prepare Campaign Input Data",
            input_path="$.solutionVersionArn",  # NOSONAR (python:S1192) - string for clarity
            result_path="$.campaign.serviceConfig.solutionVersionArn",
        )

        _create_solution = create_solution.state(
            self,
            "Create Solution",
            result_path=TEMPORARY_PATH,
            input_path="$.solution",
            result_selector={
                "solutionArn.$": "$.solutionArn"
            }
        )
        _create_recommender = create_recommender.state(
            self,
            "Create Recommender",
            result_path=TEMPORARY_PATH,
            input_path="$.recommender",
            result_selector={
                "recommenderArn.$": "$.recommenderArn"
            },
            **retry_config
        )

        _create_solution_version = create_solution_version.state(
            self,
            "Create Solution Version",
            result_path=TEMPORARY_PATH,
            input_path="$.solution.solutionVersion",
            result_selector={
                "solutionVersionArn.$": "$.solutionVersionArn"  # NOSONAR (python:S1192) - string for clarity
            },
            **retry_config,
        )
        _create_solution_version.task.add_catch(
            Pass(
                self,
                "Save Solution Version ID",
                parameters={
                    "errorInfo.$": "States.StringToJson($.solutionVersionPending.Cause)"
                },
                result_path=TEMPORARY_PATH
            ).next(
                Pass(
                    self,
                    "Set Solution Version ID",
                    parameters={
                        "serviceConfig": {
                            "trainingMode.$": "$.solution.solutionVersion.serviceConfig.trainingMode",
                            "solutionArn.$": "$.solution.solutionVersion.serviceConfig.solutionArn",  # NOSONAR (python:S1192) - string for clarity
                        },
                        "workflowConfig": {
                            "maxAge.$": "$.solution.solutionVersion.workflowConfig.maxAge",
                            "solutionVersionArn.$": f"{TEMPORARY_PATH}.errorInfo.errorMessage",
                            "timeStarted.$": "$$.State.EnteredTime",
                        }
                    },
                    result_path="$.solution.solutionVersion"
                )
            ).next(_create_solution_version),
            errors=["SolutionVersionPending"],
            result_path="$.solutionVersionPending"
        )

        _create_campaign = create_campaign.state(
            self,
            "Create Campaign",
            result_path="$.campaign.serviceConfig",
            input_path="$.campaign",
            **retry_config,
        )

        _create_batch_inference_jobs = BatchInferenceJobsFragment(
            self,
            "Create Batch Inference Jobs",
            create_batch_inference_job=create_batch_inference_job,
            scheduler=scheduler,
            to_schedule=to_schedule,
        ).start_state

        _create_batch_segment_jobs = BatchSegmentJobsFragment(
            self,
            "Create Batch Segment Jobs",
            create_batch_segment_job=create_batch_segment_job,
            scheduler=scheduler,
            to_schedule=to_schedule,
        ).start_state

        self.create_campaigns = campaigns_available.when(
            Condition.is_present("$.solution.campaigns[0]"),
            Map(
                self,
                "Create Campaigns",
                items_path="$.solution.campaigns",  # NOSONAR (python:S1192) - string for clarity
                parameters={
                    "solutionVersionArn.$": "$.solution.solutionVersion.serviceConfig.solutionVersionArn",
                    "campaign.$": "$$.Map.Item.Value",
                }
            ).iterator(_prepare_campaign_input
                       .next(_create_campaign))
        )

        campaigns_and_batch = Parallel(
            self,
            "Create Campaigns and Batch Inference Jobs",
            result_path=JsonPath.DISCARD
        )
        campaigns_and_batch.branch(self.create_campaigns)
        campaigns_and_batch.branch(_create_batch_inference_jobs)
        campaigns_and_batch.branch(_create_batch_segment_jobs)
        if scheduler and to_schedule:
            campaigns_and_batch.next(
                SchedulerFragment(
                    self,
                    schedule_for="solution maintenance full",
                    schedule_for_suffix="$.solution.serviceConfig.name",  # NOSONAR (python:S1192) - string for clarity
                    scheduler=scheduler,
                    target=to_schedule,
                    schedule_path="$.solution.workflowConfig.schedules.full",
                    schedule_input={
                        "bucket.$": BUCKET_PATH,  # NOSONAR (python:S1192) - string for clarity
                        "datasetGroup": {
                            "serviceConfig": {
                                "name.$": "$.datasetGroupName",
                                "datasetGroupArn.$": "$.datasetGroupArn",  # NOSONAR (python:S1192) - string for clarity
                            }
                        },
                        "solutions": [
                            {
                                "serviceConfig.$": "$.solution.serviceConfig",
                                "solutionVersion": {
                                    "serviceConfig": {
                                        "trainingMode": "FULL",
                                        "solutionArn.$": "$.solution.solutionVersion.serviceConfig.solutionArn",  # NOSONAR (python:S1192) - string for clarity
                                    },
                                    "workflowConfig": {
                                        "maxAge": MINIMUM_TIME
                                    }
                                },
                                "campaigns.$": "$.solution.campaigns",
                            }
                        ]
                    }
                )
            ).next(
                SchedulerFragment(
                    self,
                    schedule_for="solution maintenance update",
                    schedule_for_suffix="$.solution.serviceConfig.name",  # NOSONAR (python:S1192) - string for clarity
                    scheduler=scheduler,
                    target=to_schedule,
                    schedule_path="$.solution.workflowConfig.schedules.update",
                    schedule_input={
                        "bucket.$": BUCKET_PATH,
                        "datasetGroup": {
                            "serviceConfig": {
                                "name.$": "$.datasetGroupName",
                                "datasetGroupArn.$": "$.datasetGroupArn",
                            }
                        },
                        "solutions": [
                            {
                                "serviceConfig.$": "$.solution.serviceConfig",
                                "solutionVersion": {
                                    "serviceConfig": {
                                        "trainingMode": "UPDATE",
                                        "solutionArn.$": "$.solution.solutionVersion.serviceConfig.solutionArn",  # NOSONAR (python:S1192) - string for clarity
                                    },
                                    "workflowConfig": {
                                        "maxAge": MINIMUM_TIME,
                                    }
                                },
                                "campaigns.$": "$.solution.campaigns",
                            }
                        ]
                    }
                )
            )

        _check_solution_version = Choice(self, "Check for Solution Version")
        _check_solution_version.when(
            Condition.is_not_present("$.solution.solutionVersion"),
            _prepare_solution_version_input
        )
        _check_solution_version.afterwards().next(
            _create_solution_version
            .next(_prepare_solution_version_output)
            .next(campaigns_and_batch)
        )
        _check_solution_version.otherwise(_create_solution_version)

        self._create_recommenders = Map(
            self,
            "Create Recommenders",
            items_path="$.recommenders",
            result_path=JsonPath.DISCARD,
            parameters={
                "datasetGroupArn.$": "$.datasetGroup.serviceConfig.datasetGroupArn",
                "datasetGroupName.$": "$.datasetGroup.serviceConfig.name",
                "recommender.$": "$$.Map.Item.Value",
                "bucket.$": BUCKET_PATH,
                "currentDate.$": CURRENT_DATE_PATH,  # NOSONAR (python:S1192) - string for clarity
            }
        ).iterator(_prepare_recommender_input
                   .next(_create_recommender)
        )
        self._create_solutions = Map(
            self,
            "Create Solutions",
            items_path="$.solutions",
            result_path=JsonPath.DISCARD,
            parameters={
                "datasetGroupArn.$": "$.datasetGroup.serviceConfig.datasetGroupArn",
                "datasetGroupName.$": "$.datasetGroup.serviceConfig.name",
                "solution.$": "$$.Map.Item.Value",
                "bucket.$": BUCKET_PATH,
                "currentDate.$": CURRENT_DATE_PATH,  # NOSONAR (python:S1192) - string for clarity
            }
        ).iterator(_prepare_solution_input
                   .next(_create_solution)
                   .next(_prepare_solution_output)
                   .next(_check_solution_version)
                   )

        self.recommenders_and_solutions = Parallel(self, "Recommenders and Solutions", result_path=JsonPath.DISCARD).branch(
            self.solutions_available
        ).branch(
            self.recommenders_available
        )

        self.solutions_available.when(Condition.is_present("$.solutions[0]"), self._create_solutions)
        self.recommenders_available.when(Condition.is_present("$.recommenders[0]"), self._create_recommenders)
        # fmt: on

    @property
    def start_state(self) -> State:
        return self.recommenders_and_solutions

    @property
    def end_states(self) -> List[INextable]:
        return [self.recommenders_and_solutions]
