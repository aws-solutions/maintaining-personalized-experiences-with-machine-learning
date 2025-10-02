# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from shared.resource.base import Resource
from shared.resource.dataset_import_job import DatasetImportJob


class Dataset(Resource):
    children = [DatasetImportJob()]
    allowed_types = {"INTERACTIONS", "ITEMS", "USERS"}
