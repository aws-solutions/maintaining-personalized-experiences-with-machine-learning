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

import aws_cdk.aws_iam as iam
from aws_cdk.core import Construct, Aws

from personalize.aws_lambda.functions.solutionstep import SolutionStep


class CreateCampaign(SolutionStep):
    def __init__(
        self,
        scope: Construct,
        id: str,
        layers=None,
    ):
        super().__init__(
            scope,
            id,
            layers=layers,
        )

    def _set_permissions(self):
        self.function.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "personalize:DescribeSolutionVersion",
                    "personalize:ListSolutionVersions",
                    "personalize:CreateCampaign",
                    "personalize:ListCampaigns",
                    "personalize:DescribeCampaign",
                    "personalize:UpdateCampaign",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:solution-version/*",
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:solution/*",
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:campaign/*",
                ],
            )
        )
