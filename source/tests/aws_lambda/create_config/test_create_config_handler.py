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

from aws_lambda.create_config.handler import lambda_handler
from shared.resource import (
    DatasetGroup,
    Dataset,
    Solution,
    Campaign,
    SolutionVersion,
    BatchInferenceJob,
    EventTracker,
    Schema,
    BatchSegmentJob,
)


def test_create_config(personalize_stubber):
    dsg_name = "dsg"
    event_tracker_name = "dsgeventtracker"
    dataset_name = "dsg/INTERACTIONS"
    schema_name = "dsg_interactions_schema"
    solution_name = "dsgsolution"
    campaign_name = "dsgcampaign"

    dsg_arn = DatasetGroup().arn(dsg_name)
    dataset_arn = Dataset().arn(dataset_name)
    event_tracker_arn = EventTracker().arn(event_tracker_name)
    schema_arn = Schema().arn(schema_name)
    solution_arn = Solution().arn(solution_name)
    campaign_arn = Campaign().arn(campaign_name)

    personalize_stubber.add_response(
        method="list_datasets",
        service_response={"datasets": [{"name": "dsg_interactions", "datasetArn": dataset_arn}]},
    )
    personalize_stubber.add_response(method="list_dataset_import_jobs", service_response={"datasetImportJobs": []})
    personalize_stubber.add_response(
        method="list_filters",
        service_response={"Filters": []},
        expected_params={"datasetGroupArn": dsg_arn},
    )
    personalize_stubber.add_response(
        method="list_solutions",
        service_response={"solutions": [{"solutionArn": solution_arn}]},
        expected_params={"datasetGroupArn": dsg_arn},
    )
    personalize_stubber.add_response(
        method="list_campaigns",
        service_response={"campaigns": [{"campaignArn": campaign_arn}]},
        expected_params={"solutionArn": solution_arn},
    )
    personalize_stubber.add_response(
        method="list_solution_versions",
        service_response={
            "solutionVersions": [{"solutionVersionArn": SolutionVersion().arn("dsgsolution", sv_id="aaaaaaaa")}]
        },
        expected_params={"solutionArn": solution_arn},
    )
    personalize_stubber.add_response(
        method="list_batch_inference_jobs",
        service_response={"batchInferenceJobs": [{"batchInferenceJobArn": BatchInferenceJob().arn("dsgbatch")}]},
    )
    personalize_stubber.add_response(
        method="list_batch_segment_jobs",
        service_response={"batchSegmentJobs": [{"batchSegmentJobArn": BatchSegmentJob().arn("dsgbatch")}]},
    )
    personalize_stubber.add_response(
        method="list_recommenders",
        expected_params={"datasetGroupArn": dsg_arn},
        service_response={"recommenders": []},
    )
    personalize_stubber.add_response(
        method="list_event_trackers",
        service_response={"eventTrackers": [{"eventTrackerArn": event_tracker_arn}]},
        expected_params={"datasetGroupArn": dsg_arn},
    )
    personalize_stubber.add_response(
        method="describe_dataset_group",
        service_response={"datasetGroup": {"name": dsg_name, "datasetGroupArn": dsg_arn}},
        expected_params={"datasetGroupArn": dsg_arn},
    )
    personalize_stubber.add_response(
        method="describe_event_tracker",
        service_response={
            "eventTracker": {
                "name": event_tracker_name,
                "eventTrackerArn": event_tracker_arn,
            }
        },
        expected_params={"eventTrackerArn": event_tracker_arn},
    )
    personalize_stubber.add_response(
        method="describe_dataset",
        service_response={
            "dataset": {
                "name": dataset_name,
                "datasetArn": dataset_arn,
                "schemaArn": schema_arn,
            }
        },
        expected_params={"datasetArn": dataset_arn},
    )
    personalize_stubber.add_response(
        method="describe_schema",
        service_response={"schema": {"name": schema_name, "schemaArn": schema_arn, "schema": "{}"}},
        expected_params={"schemaArn": schema_arn},
    )
    personalize_stubber.add_response(
        method="describe_solution",
        service_response={"solution": {"name": solution_name, "solutionArn": solution_arn}},
        expected_params={"solutionArn": solution_arn},
    )
    personalize_stubber.add_response(
        method="describe_campaign",
        service_response={
            "campaign": {
                "name": campaign_name,
                "campaignArn": campaign_arn,
            }
        },
        expected_params={"campaignArn": campaign_arn},
    )

    result = lambda_handler(
        {
            "datasetGroupName": dsg_name,
            "schedules": {
                "import": "cron(0 */6 * * ? *)",
                "solutions": {
                    solution_name: {
                        "full": "cron(0 0 ? * 1 *)",
                        "update": "cron(0 * * * ? *)",
                    }
                },
            },
        },
        None,
    )
    assert result["datasetGroup"]["serviceConfig"]["name"] == dsg_name
    assert result["datasetGroup"]["workflowConfig"]["schedules"]["import"] == "cron(0 */6 * * ? *)"
    assert result["eventTracker"]["serviceConfig"]["name"] == event_tracker_name
    assert not result.get("filters")
    assert len(result["solutions"]) == 1
    assert result["solutions"][0]["serviceConfig"]["name"] == solution_name
    assert result["solutions"][0]["workflowConfig"]["schedules"]["full"] == "cron(0 0 ? * 1 *)"
    assert result["solutions"][0]["workflowConfig"]["schedules"]["update"] == "cron(0 * * * ? *)"
    assert len(result["solutions"][0]["campaigns"]) == 1
    assert result["solutions"][0]["campaigns"][0]["serviceConfig"]["name"] == campaign_name
