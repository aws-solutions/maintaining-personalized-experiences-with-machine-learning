# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from aws_solutions.cdk.context import SolutionContext
from aws_solutions.cdk.stack import SolutionStack
from aws_solutions.cdk.synthesizers import SolutionStackSubstitutions


class CDKSolution:
    """
    A CDKSolution stores helper utilities for building AWS Solutions using the AWS CDK in Python

    :type cdk_json_path: Path
    :param cdk_json_path: The full path to the cdk.json context for your application
    :type qualifier: str
    :param qualifier: A string that is added to all resources in the CDK bootstrap stack. The default value has no significance.
    """

    def __init__(self, cdk_json_path: Path, qualifier="hnb659fds"):
        self.qualifier = qualifier
        self.context = SolutionContext(cdk_json_path=cdk_json_path)
        self.synthesizer = SolutionStackSubstitutions(qualifier=self.qualifier)

    def reset(self) -> None:
        """
        Get a new synthesizer for this CDKSolution - useful for testing
        :return: None
        """
        self.synthesizer = SolutionStackSubstitutions(qualifier=self.qualifier, generate_bootstrap_version_rule=False)
