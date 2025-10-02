# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from aws_cdk import App, Stack, Aspects, CfnCondition, Fn
from aws_cdk.aws_sqs import Queue, CfnQueue
from constructs import Construct

from aws_solutions.cdk.aspects import ConditionalResources


class SomeConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        q1 = Queue(self, "TestQueue1")
        q1.node.default_child.override_logical_id("TestQueue1")
        q2 = Queue(self, "TestQueue2")
        q2.node.default_child.override_logical_id("TestQueue2")
        q3 = CfnQueue(self, "TestQueu3")
        q3.override_logical_id("TestQueue3")


class SomeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        condition = CfnCondition(self, "SomeCondition", expression=Fn.condition_equals("1", "1"))
        queues = SomeConstruct(self, "SomeQueues")
        Aspects.of(queues).add(ConditionalResources(condition))


@pytest.fixture
def stack_conditional():
    app = App()
    SomeStack(app, "some-test-queues")
    yield app.synth().get_stack_by_name("some-test-queues").template


def test_conditional_resources(stack_conditional):
    assert stack_conditional["Conditions"]["SomeCondition"]["Fn::Equals"] == [
        "1",
        "1",
    ]
    for k, v in stack_conditional["Resources"].items():
        assert v["Condition"] == "SomeCondition"
