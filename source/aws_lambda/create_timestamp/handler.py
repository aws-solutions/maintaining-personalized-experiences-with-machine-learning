# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
from typing import Dict, Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event: Dict[str, Any], context: LambdaContext) -> str:
    """Create a timestamp matching YYYY_mm_dd_HH_MM_SS
    :param event: AWS Lambda Event
    :param context: AWS Lambda Context
    :return: the timestamp (string)
    """
    return datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
