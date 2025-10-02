# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from aws_lambda.create_schema.handler import CONFIG, RESOURCE, lambda_handler


def test_create_schema_handler(validate_handler_config):
    validate_handler_config(RESOURCE, CONFIG)
    with pytest.raises(ValueError):
        lambda_handler({}, None)
