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

from aws_cdk.aws_lambda import Tracing, Runtime, RuntimeFamily
from aws_cdk.aws_sns import Topic
from aws_cdk.core import Construct, Duration

from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from personalize.aws_lambda.functions.environment import Environment


class SNSNotification(SolutionsPythonFunction):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        entrypoint = (
            Path(__file__).absolute().parents[4]
            / "aws_lambda"
            / "sns_notification"
            / "handler.py"
        )
        function = "lambda_handler"
        kwargs["tracing"] = Tracing.ACTIVE
        kwargs["timeout"] = Duration.seconds(15)
        kwargs["runtime"] = Runtime("python3.9", RuntimeFamily.PYTHON)

        super().__init__(scope, construct_id, entrypoint, function, **kwargs)

        self.environment = Environment(self)

        add_cfn_nag_suppressions(
            self.role.node.try_find_child("DefaultPolicy").node.find_child("Resource"),
            [
                CfnNagSuppression(
                    "W12", "IAM policy for AWS X-Ray requires an allow on *"
                )
            ],
        )

    def grant_publish(self, topic: Topic):
        topic.grant_publish(self)
        self.add_environment("SNS_TOPIC_ARN", topic.topic_arn)
