# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from shared.resource.base import Resource
from shared.resource.campaign import Campaign
from shared.resource.solution_version import SolutionVersion


class Solution(Resource):
    children = [Campaign(), SolutionVersion()]
