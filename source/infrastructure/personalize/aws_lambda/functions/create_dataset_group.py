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

from typing import Optional

import aws_cdk.aws_iam as iam
from aws_cdk.aws_stepfunctions import IChainable
from aws_cdk.core import Construct, Aws, CfnCondition, CfnParameter, Fn

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.resource_hash import ResourceHash
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from personalize.aws_lambda.functions.solutionstep import SolutionStep


class CreateDatasetGroup(SolutionStep):
    def __init__(
        self,  # NOSONAR (python:S107) - allow large number of method parameters
        scope: Construct,
        id: str,
        kms_enabled: CfnCondition,
        kms_key: CfnParameter,
        layers=None,
        failure_state: Optional[IChainable] = None,
        **kwargs,
    ):
        self.kms_enabled = kms_enabled
        self.kms_key = kms_key

        super().__init__(
            scope,
            id,
            layers=layers,
            failure_state=failure_state,
            **kwargs,
        )

    def _set_permissions(self):
        """
        Set up a role to allow Amazon Personalize to access a KMS key in this account.
        The role name must be established beforehand and is based on a hash of the StackId
        For example (upper(md5(arn:aws:cloudformation:us-west-2:<id>>:stack/stack-name)

        This requires the use of the CloudFormation CAPABILITY_NAMED_IAM permission but means
        the name of the role will be maintained across the same named deployment of the stack,
        but be different regionally (and by partition). Without this set, removing and re-adding KMS
        permission to the stack would result in a different role name being created, which would make
        resources in Amazon Personalize inaccessible to this solution.

        Note that Personalize must also be granted access to the KMS key via key policy to use the
        parameter. Consult the implementation guide for an example key policy.
        :return: None
        """
        hashed_name = ResourceHash(
            self,
            "KmsResourceNameHash",
            purpose="PersonalizeKMSReadWriteRole",
            max_length=64,
        )

        kms_role = iam.Role(
            self,
            "PersonalizeKMSReadWriteRole",
            role_name=hashed_name.resource_name.to_string(),
            description="Grants Amazon Personalize access to use the specified KMS Key for SSE-KMS",
            assumed_by=iam.ServicePrincipal("personalize.amazonaws.com"),
            inline_policies={
                "PersonalizeKmsWriteAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "kms:Encrypt",
                                "kms:ReEncrypt",
                                "kms:Decrypt",
                                "kms:CreateGrant",
                                "kms:RevokeGrant",
                                "kms:RetireGrant",
                                "kms:ListGrants",
                                "kms:GenerateDataKey",
                                "kms:DescribeKey",
                            ],
                            resources=[self.kms_key.value_as_string],
                        )
                    ]
                ),
                "PersonalizeKmsReadAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "kms:Decrypt",
                                "kms:DescribeKey",
                                "kms:GenerateDataKey",
                            ],
                            resources=[self.kms_key.value_as_string],
                        )
                    ],
                ),
            },
        )
        kms_role.node.default_child.cfn_options.condition = self.kms_enabled
        add_cfn_nag_suppressions(
            kms_role.node.default_child,
            [
                CfnNagSuppression(
                    "W28",
                    "Resource requires consistent name across stack deployments and is unique per region + stack",
                ),
            ],
        )

        # Grant the function access to Amazon Personalize
        self.function.add_to_role_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "personalize:DescribeDatasetGroup",
                    "personalize:CreateDatasetGroup",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:{Aws.PARTITION}:personalize:{Aws.REGION}:{Aws.ACCOUNT_ID}:dataset-group/*"
                ],
            )
        )

        # Grant the function access to pass the role
        kms_role_passrole = iam.ManagedPolicy(
            self,
            "PersonalizeKmsPassRole",
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["iam:PassRole"],
                        resources=[kms_role.role_arn],
                    )
                ]
            ),
            roles=[self.function.role],
        )
        kms_role_passrole.node.default_child.cfn_options.condition = self.kms_enabled

        self.function.add_environment(
            "KMS_ROLE_ARN",
            Fn.condition_if(
                self.kms_enabled.node.id, kms_role.role_arn, ""
            ).to_string(),
        )
        self.function.add_environment(
            "KMS_KEY_ARN",
            Fn.condition_if(
                self.kms_enabled.node.id, self.kms_key.value_as_string, ""
            ).to_string(),
        )
