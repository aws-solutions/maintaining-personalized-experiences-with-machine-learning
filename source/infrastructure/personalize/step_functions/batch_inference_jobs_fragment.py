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
    Chain,
    StateMachine,
)
from constructs import Construct

from aws_solutions.scheduler.cdk.construct import Scheduler
from aws_solutions.scheduler.cdk.scheduler_fragment import SchedulerFragment
from personalize.aws_lambda.functions import (
    CreateBatchInferenceJob,
)

TEMPORARY_PATH = "$._tmp"
BATCH_INFERENCE_JOB_PATH = "$.batchInferenceJob"
BUCKET_PATH = "$.bucket"
CURRENT_DATE_PATH = "$.currentDate"


class BatchInferenceJobsFragment(StateMachineFragment):
    def __init__(
        self,
        scope: Construct,
        id: str,
        create_batch_inference_job: CreateBatchInferenceJob,
        scheduler: Optional[Scheduler] = None,
        to_schedule: Optional[StateMachine] = None,
    ):
        super().__init__(scope, id)

        # total allowed elapsed duration ~ 5h
        retry_config = {
            "backoff_rate": 1.02,
            "interval": Duration.seconds(60),
            "max_attempts": 100,
        }

        self.batch_inference_jobs_not_available = Pass(
            self, "Batch Inference Jobs Not Provided"
        )
        batch_inference_jobs_available = Choice(
            self, "Check for Batch Inference Jobs"
        ).otherwise(self.batch_inference_jobs_not_available)

        _prepare_batch_inference_job_input_job_name = Pass(
            self,
            "Set Batch Inference Job Input Data - Job Name",
            input_path="$.batchInferenceJobName",
            result_path=f"{BATCH_INFERENCE_JOB_PATH}.serviceConfig.jobName",
        )

        _prepare_batch_inference_job_input_solution_version_arn = Pass(
            self,
            "Set Batch Inference Job Input Data - Solution Version ARN",
            input_path="$.solutionVersionArn",  # NOSONAR (python:S1192) - string for clarity
            result_path=f"{BATCH_INFERENCE_JOB_PATH}.serviceConfig.solutionVersionArn",
        )

        _prepare_batch_inference_job_job_input = Pass(
            self,
            "Set Batch Inference Job Input Data - Job Input",
            result_path=f"{BATCH_INFERENCE_JOB_PATH}.serviceConfig.jobInput",
            parameters={
                "s3DataSource": {
                    "path.$": f"States.Format('s3://{{}}/batch/{{}}/{{}}/job_config.json', $.bucket.name, $.datasetGroupName, $.solution.serviceConfig.name)"  # NOSONAR (python:S1192) - string for clarity
                }
            },
        )

        _prepare_batch_inference_job_job_output = Pass(
            self,
            "Set Batch Inference Job Input Data - Job Output",
            result_path=f"{BATCH_INFERENCE_JOB_PATH}.serviceConfig.jobOutput",
            parameters={
                "s3DataDestination": {
                    "path.$": f"States.Format('s3://{{}}/batch/{{}}/{{}}/{{}}/', $.bucket.name, $.datasetGroupName, $.solution.serviceConfig.name, $.batchInferenceJobName)"  # NOSONAR (python:S1192) - string for clarity
                }
            },
        )

        _prepare_batch_inference_job_input = Chain.start(
            _prepare_batch_inference_job_input_job_name.next(
                _prepare_batch_inference_job_input_solution_version_arn
            )
            .next(_prepare_batch_inference_job_job_input)
            .next(_prepare_batch_inference_job_job_output)
        )

        _create_batch_inference_job = create_batch_inference_job.state(
            self,
            "Create Batch Inference Job",
            result_path=f"{BATCH_INFERENCE_JOB_PATH}.serviceConfig",
            input_path=f"{BATCH_INFERENCE_JOB_PATH}",
            **retry_config,
        )
        if scheduler and to_schedule:
            _create_batch_inference_job.next(
                SchedulerFragment(
                    self,
                    schedule_for="batch inference",
                    schedule_for_suffix="$.solution.serviceConfig.name",  # NOSONAR (python:S1192) - string for clarity
                    scheduler=scheduler,
                    target=to_schedule,
                    schedule_path="$.batchInferenceJob.workflowConfig.schedule",
                    schedule_input={
                        "bucket.$": "$.bucket",
                        "datasetGroup": {
                            "serviceConfig": {
                                "name.$": "$.datasetGroupName",
                                "datasetGroupArn.$": "$.datasetGroupArn",
                            }
                        },
                        "solutions": [
                            {
                                "serviceConfig.$": "$.solution.serviceConfig",
                                "batchInferenceJobs": [
                                    {
                                        "serviceConfig.$": "$.batchInferenceJob.serviceConfig",
                                        "workflowConfig": {"maxAge": "1 second"},
                                    }
                                ],
                            }
                        ],
                    },
                )
            )

        self.create_batch_inference_jobs = batch_inference_jobs_available.when(
            Condition.is_present("$.solution.batchInferenceJobs[0]"),
            Map(
                self,
                "Create Batch Inference Jobs",
                items_path="$.solution.batchInferenceJobs",
                parameters={
                    "solutionVersionArn.$": "$.solution.solutionVersion.serviceConfig.solutionVersionArn",
                    "batchInferenceJob.$": "$$.Map.Item.Value",
                    "batchInferenceJobName.$": f"States.Format('batch_{{}}_{{}}', $.solution.serviceConfig.name, {CURRENT_DATE_PATH})",
                    "bucket.$": BUCKET_PATH,  # NOSONAR (python:S1192) - string for clarity
                    "currentDate.$": CURRENT_DATE_PATH,  # NOSONAR (python:S1192) - string for clarity
                    "datasetGroupName.$": "$.datasetGroupName",
                    "datasetGroupArn.$": "$.datasetGroupArn",
                    "solution.$": "$.solution",
                },
            ).iterator(
                _prepare_batch_inference_job_input.next(_create_batch_inference_job)
            ),
        )

    @property
    def start_state(self) -> State:
        return self.create_batch_inference_jobs.start_state

    @property
    def end_states(self) -> List[INextable]:
        return [
            self.create_batch_inference_jobs.start_state,
            self.batch_inference_jobs_not_available,
        ]
