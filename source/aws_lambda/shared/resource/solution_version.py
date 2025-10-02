# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from shared.resource.base import Resource
from shared.resource.batch_inference_job import BatchInferenceJob
from shared.resource.batch_segment_job import BatchSegmentJob


class SolutionVersion(Resource):
    children = [BatchInferenceJob(), BatchSegmentJob()]
    has_soft_limit = True
