# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from shared.resource.base import Resource
from shared.resource.dataset import Dataset
from shared.resource.event_tracker import EventTracker
from shared.resource.filter import Filter
from shared.resource.recommender import Recommender
from shared.resource.solution import Solution


class DatasetGroup(Resource):
    children = [Dataset(), Filter(), Solution(), Recommender(), EventTracker()]
