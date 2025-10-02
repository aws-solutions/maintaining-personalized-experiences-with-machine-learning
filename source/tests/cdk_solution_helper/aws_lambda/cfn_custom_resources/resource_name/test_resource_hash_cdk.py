# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from aws_cdk import Stack, App
from constructs import Construct

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.resource_name.name import (
    ResourceName,
)


class SomeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.name_1 = ResourceName(self, "name_1", purpose="var_1", max_length=32)
        self.name_2 = ResourceName(self, "name_2", purpose="var_2", max_length=32)


@pytest.fixture
def resource_naming_stack():
    app = App()
    SomeStack(app, "some-test-naming")
    yield app.synth().get_stack_by_name("some-test-naming").template


def test_resource_service_tokens(resource_naming_stack):
    # There should be only one lambda function generated.
    service_tokens = [
        resource["Properties"]["ServiceToken"]
        for resource in resource_naming_stack["Resources"].values()
        if resource["Type"] == "Custom::ResourceName"
    ]
    assert all(st == service_tokens[0] for st in service_tokens)
