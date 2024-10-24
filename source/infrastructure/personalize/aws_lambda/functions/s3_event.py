# ######################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                  #
#                                                                                                                      #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance      #
#  with the License. You may obtain a copy of the License at                                                           #
#                                                                                                                      #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                      #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed    #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for   #
#  the specific language governing permissions and limitations under the License.                                      #
# ######################################################################################################################

from pathlib import Path

from aws_cdk import Duration
from aws_cdk.aws_lambda import Tracing, Runtime, RuntimeFamily
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_sns import Topic
from aws_cdk.aws_stepfunctions import StateMachine
from constructs import Construct

from aws_solutions.cdk.aws_lambda.environment import Environment
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from aws_solutions.cdk.cfn_guard import add_cfn_guard_suppressions

class S3EventHandler(SolutionsPythonFunction):
    def __init__(
        self, scope: Construct, construct_id: str, state_machine: StateMachine, bucket: Bucket, topic: Topic, **kwargs
    ):
        entrypoint = Path(__file__).absolute().parents[4] / "aws_lambda" / "s3_event" / "handler.py"
        function_name = "lambda_handler"
        kwargs["libraries"] = [Path(__file__).absolute().parents[4] / "aws_lambda" / "shared"]
        kwargs["tracing"] = Tracing.ACTIVE
        kwargs["timeout"] = Duration.seconds(15)
        kwargs["runtime"] = Runtime("python3.11", RuntimeFamily.PYTHON)

        super().__init__(scope, construct_id, entrypoint, function_name, **kwargs)

        self.environment = Environment(self)
        self.add_environment("STATE_MACHINE_ARN", state_machine.state_machine_arn)

        add_cfn_nag_suppressions(
            self.role.node.try_find_child("DefaultPolicy").node.find_child("Resource"),
            [CfnNagSuppression("W12", "IAM policy for AWS X-Ray requires an allow on *")],
        )

        add_cfn_guard_suppressions(
         self.role.node.try_find_child("Resource"),
          ["IAM_NO_INLINE_POLICY_CHECK"]
        )

        bucket.grant_read(self, objects_key_pattern="train/*")
        state_machine.grant_start_execution(self)

        self.grant_publish(topic)

    def grant_publish(self, topic: Topic):
        topic.grant_publish(self)
        self.add_environment("SNS_TOPIC_ARN", topic.topic_arn)
