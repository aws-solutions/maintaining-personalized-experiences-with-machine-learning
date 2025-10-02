# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from shared.resource.base import Resource
from shared.resource.batch_inference_job import BatchInferenceJob
from shared.resource.batch_segment_job import BatchSegmentJob
from shared.resource.campaign import Campaign
from shared.resource.dataset import Dataset
from shared.resource.dataset_group import DatasetGroup
from shared.resource.dataset_import_job import DatasetImportJob
from shared.resource.event_tracker import EventTracker
from shared.resource.filter import Filter
from shared.resource.recommender import Recommender
from shared.resource.schema import Schema
from shared.resource.solution import Solution
from shared.resource.solution_version import SolutionVersion


def get_resource(resource_type: str) -> Resource:
    return {
        "datasetGroup": DatasetGroup(),
        "schema": Schema(),
        "dataset": Dataset(),
        "datasetImportJob": DatasetImportJob(),
        "solution": Solution(),
        "solutionVersion": SolutionVersion(),
        "campaign": Campaign(),
        "eventTracker": EventTracker(),
        "filter": Filter(),
        "batchInferenceJob": BatchInferenceJob(),
        "batchSegmentJob": BatchSegmentJob(),
        "recommender": Recommender(),
    }[resource_type]


MANAGED_RESOURCES = [
    DatasetGroup(),
    Schema(),
    Dataset(),
    DatasetImportJob(),
    Solution(),
    SolutionVersion(),
    Campaign(),
    EventTracker(),
    Filter(),
    BatchInferenceJob(),
    BatchSegmentJob(),
    Recommender(),
]
