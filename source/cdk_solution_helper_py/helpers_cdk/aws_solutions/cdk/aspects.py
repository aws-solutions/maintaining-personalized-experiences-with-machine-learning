# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import jsii
from aws_cdk import CfnCondition, IAspect
from constructs import IConstruct


@jsii.implements(IAspect)
class ConditionalResources:
    """Mark any CDK construct as conditional (this is useful to apply to stacks and L2+ constructs)"""

    def __init__(self, condition: CfnCondition):
        self.condition = condition

    def visit(self, node: IConstruct):
        if "is_cfn_element" in dir(node) and node.is_cfn_element(node):
            node.cfn_options.condition = self.condition
        elif "is_cfn_element" in dir(node.node.default_child):
            node.node.default_child.cfn_options.condition = self.condition
