# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

from aws_cdk.aws_stepfunctions import StateMachineFragment


@dataclass
class Schedules:
    dataset_import: StateMachineFragment
