# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
