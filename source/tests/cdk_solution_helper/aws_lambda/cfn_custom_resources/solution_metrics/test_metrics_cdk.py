# #####################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                 #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. You may obtain a copy of the License at                                                          #
#                                                                                                                     #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                        #
#                                                                                                                     #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed   #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for  #
#  the specific language governing permissions and limitations under the License.                                     #
# #####################################################################################################################

import pytest
from aws_cdk import Stack, App
from constructs import Construct

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.solutions_metrics.metrics import (
    Metrics,
)

ADDITIONAL_METRICS_VALID = {
    "one": 1,
    "two": {"three": "3"},
}


class SomeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Metrics(self, construct_id, dict(**ADDITIONAL_METRICS_VALID))


@pytest.fixture
def test_stack_metrics():
    app = App()
    SomeStack(app, "some-test-metrics")
    yield app.synth().get_stack_by_name("some-test-metrics").template


def test_metrics_valid(test_stack_metrics):
    metric_resource = test_stack_metrics["Resources"]["SolutionMetricsAnonymousData"]

    assert metric_resource["Type"] == "Custom::AnonymousData"
    assert all(metric_resource["Properties"][k] == v for k, v in ADDITIONAL_METRICS_VALID.items())
    assert metric_resource["Properties"]["Region"]["Ref"] == "AWS::Region"
