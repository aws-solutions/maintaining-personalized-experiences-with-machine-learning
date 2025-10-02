# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Any

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.utilities.typing import LambdaContext

from shared.sfn_middleware import set_workflow_config

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event: Dict[str, Any], _) -> Dict:
    """Add timeStarted to the workflowConfig of all items
    :param event: AWS Lambda Event
    :param context: AWS Lambda Context
    :return: the modified input
    """
    config = set_workflow_config(event)
    return config
