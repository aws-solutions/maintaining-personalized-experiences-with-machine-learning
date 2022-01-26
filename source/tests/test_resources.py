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

import pytest

from shared.resource import (
    DatasetGroup,
    Schema,
    Dataset,
    DatasetImportJob,
    Solution,
    SolutionVersion,
    Campaign,
    EventTracker,
    BatchSegmentJob,
    BatchInferenceJob,
)


@pytest.mark.parametrize(
    "klass,camel,dash,snake",
    [
        (DatasetGroup, "datasetGroup", "dataset-group", "dataset_group"),
        (Schema, "schema", "schema", "schema"),
        (Dataset, "dataset", "dataset", "dataset"),
        (
            DatasetImportJob,
            "datasetImportJob",
            "dataset-import-job",
            "dataset_import_job",
        ),
        (Solution, "solution", "solution", "solution"),
        (SolutionVersion, "solutionVersion", "solution-version", "solution_version"),
        (Campaign, "campaign", "campaign", "campaign"),
        (EventTracker, "eventTracker", "event-tracker", "event_tracker"),
        (
            BatchInferenceJob,
            "batchInferenceJob",
            "batch-inference-job",
            "batch_inference_job",
        ),
        (BatchSegmentJob, "batchSegmentJob", "batch-segment-job", "batch_segment_job"),
    ],
    ids=[
        "DatasetGroup",
        "Schema",
        "Dataset",
        "DatasetImportJob",
        "Solution",
        "SolutionVersion",
        "Campaign",
        "EventTracker",
        "BatchInferenceJob",
        "BatchSegmentJob,",
    ],
)
def test_resource_naming(klass, camel, dash, snake):
    assert klass().name.camel == camel
    assert klass().name.dash == dash
    assert klass().name.snake == snake
