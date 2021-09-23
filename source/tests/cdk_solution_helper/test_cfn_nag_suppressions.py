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

from aws_cdk.core import CfnResource, App, Stack

from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression


def test_cfn_nag_suppression():
    rule_id = "W10"
    reason = "some reason"
    sup = CfnNagSuppression(rule_id=rule_id, reason=reason)

    assert sup.rule_id == rule_id
    assert sup.reason == reason


def test_add_cfn_nag_suppression():
    app = App()
    stack = Stack(app)
    resource = CfnResource(stack, "test", type="Custom::Test")

    add_cfn_nag_suppressions(
        resource,
        [
            CfnNagSuppression(rule_id="W1", reason="reason 1"),
            CfnNagSuppression("W2", "reason 2"),
        ],
    )

    assert resource.get_metadata("cfn_nag") == {
        "rules_to_suppress": [
            {"id": "W1", "reason": "reason 1"},
            {"id": "W2", "reason": "reason 2"},
        ]
    }
