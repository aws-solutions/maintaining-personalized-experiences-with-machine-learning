# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_cdk import CfnResource, App, Stack

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
